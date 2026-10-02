"""Decode session input frames into a child's byte pipe, including explicit EOF."""
import base64
import os
import subprocess
import sys
import threading

child = subprocess.Popen(sys.argv[1:], stdin=subprocess.PIPE)
failed = threading.Event()


def feed():
    try:
        while True:
            # 16 KiB bytes become less than 22 KiB of base64. Never accumulate
            # an unbounded line on a broken or misbehaving transport.
            frame = sys.stdin.buffer.readline(32768)
            if frame == b'E\n':
                child.stdin.close()
                return
            if not frame.startswith(b'D') or not frame.endswith(b'\n'):
                raise ValueError('invalid stdin frame')
            child.stdin.write(base64.b64decode(frame[1:-1], validate=True))
            child.stdin.flush()
    except BrokenPipeError:
        # A command such as head is allowed to stop consuming input early.
        return
    except Exception:
        failed.set()
        child.kill()


threading.Thread(target=feed, daemon=True).start()
status = child.wait()
# The input thread may still be blocked when the child exits early. Avoid
# interpreter shutdown waiting on its stdin lock; all child output is inherited.
os._exit(74 if failed.is_set() else status if status >= 0 else 128 - status)
