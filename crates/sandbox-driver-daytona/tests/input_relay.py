"""Local protocol tests for the exact relay embedded by the Daytona driver."""
import base64
from pathlib import Path
import subprocess
import sys
import unittest

RELAY = Path(__file__).resolve().parents[1] / 'src/input_relay.py'


class RelayTests(unittest.TestCase):
    def run_relay(self, program, frames):
        return subprocess.run([sys.executable, str(RELAY), sys.executable, '-c', program],
                              input=frames, capture_output=True, timeout=5)

    def test_binary_and_explicit_eof(self):
        data = bytes(range(256)) * 32
        frames = b''.join(b'D' + base64.b64encode(data[i:i+1024]) + b'\n'
                          for i in range(0, len(data), 1024)) + b'E\n'
        result = self.run_relay('import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())', frames)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, data)

    def test_missing_eof_fails(self):
        result = self.run_relay('import sys; sys.stdin.buffer.read()', b'DYQ==\n')
        self.assertEqual(result.returncode, 74)

    def test_invalid_frame_fails(self):
        self.assertEqual(self.run_relay('import sys; sys.stdin.buffer.read()', b'D!\n').returncode, 74)

    def test_early_exit_does_not_wait_for_input(self):
        with subprocess.Popen([sys.executable, str(RELAY), sys.executable, '-c', 'raise SystemExit(7)'],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
            self.assertEqual(child.wait(timeout=5), 7)

    def test_output_before_input_closes(self):
        program = 'import sys; assert sys.stdin.buffer.read(4)==b"PING"; print("ACK",flush=True); assert sys.stdin.buffer.read()==b"tail"'
        with subprocess.Popen([sys.executable, str(RELAY), sys.executable, '-c', program],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
            child.stdin.write(b'DUElORw==\n');child.stdin.flush()
            import select
            self.assertTrue(select.select([child.stdout], [], [], 5)[0])
            self.assertEqual(child.stdout.readline(), b'ACK\n')
            child.stdin.write(b'DdGFpbA==\nE\n');child.stdin.flush()
            self.assertEqual(child.wait(timeout=5), 0)


if __name__ == '__main__':
    unittest.main()
