//! The session API accepts text but cannot close stdin. Frame binary chunks
//! and an explicit EOF for a Python relay that owns the child's real pipe.
//! Python 3 must be present in the sandbox when streamed stdin is requested.
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use daytona_sdk::ProcessService;
use sandbox_driver::{Error, Result, StdinReader};
use tokio::io::AsyncReadExt as _;

use crate::sdk::daytona_error;
use crate::shell::shell_quote;

const RELAY: &str = include_str!("input_relay.py");
const CHUNK_BYTES: usize = 16 * 1024;

pub(crate) fn command(command: &str) -> String {
    format!(
        "python3 -u -c {} /bin/bash -c {}",
        shell_quote(RELAY),
        shell_quote(command)
    )
}

pub(crate) async fn feed(
    process: ProcessService,
    session: &str,
    command: &str,
    mut reader: StdinReader,
) -> Result<()> {
    let mut bytes = vec![0; CHUNK_BYTES];
    loop {
        let size = reader
            .read(&mut bytes)
            .await
            .map_err(|error| Error::io("reading exec stdin", error))?;
        let frame = if size == 0 {
            "E\n".to_owned()
        } else {
            format!("D{}\n", STANDARD.encode(&bytes[..size]))
        };
        process
            .send_session_command_input(session, command, &frame)
            .await
            .map_err(|error| daytona_error("forwarding exec stdin", error))?;
        if size == 0 {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    #[test]
    fn relay_protocol() {
        let status = Command::new("python3")
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/input_relay.py"))
            .status()
            .expect("Python 3 for the relay contract tests");
        assert!(status.success());
    }
}
