//! Opt-in live coverage: bounded input, interactive input/EOF, early exit and
//! timeout.
use std::env;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use sandbox_driver::{
    ExecControls, ExecSpec, SandboxProvider, SandboxSource, SandboxSpec, SnapshotId, StdinSource,
    Termination,
};
use sandbox_driver_daytona::DaytonaProvider;
use tokio::io::{self, AsyncWriteExt as _};
use tokio::sync::Notify;
use tokio::time;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn streamed_input_on_daytona() -> anyhow::Result<()> {
    if env::var("DAYTONA_API_KEY").is_err() {
        return Ok(());
    }
    let provider = DaytonaProvider::connect().await?;
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new("daytona-medium")?,
    })
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await?;
    let outcome = async {
        ensure!(sandbox.capabilities().exec.stdin_stream);
        let data: Vec<u8> = (0..=255).cycle().take(4096).collect();
        let result = sandbox.exec().run_streaming(
            &ExecSpec::new("cat").timeout(Duration::from_secs(60)),
            ExecControls { stdin: Some(StdinSource::new(Cursor::new(data.clone()))),
                          retained_output_limit: Some(8192), ..ExecControls::default() }
        ).await?;
        ensure!(result.result.success());
        ensure!(result.result.stdout == data, "binary input changed");

        let (mut writer, reader) = io::duplex(1024);
        let ready = Arc::new(Notify::new());
        let sink_ready = Arc::clone(&ready);
        let command = ExecSpec::new("python3").args(["-u", "-c",
            "import sys; assert sys.stdin.buffer.read(4)==b'PING'; print('ACK',flush=True); assert sys.stdin.buffer.read()==b'tail'"])
            .timeout(Duration::from_secs(60));
        let exec = sandbox.exec().run_streaming(&command, ExecControls {
            stdin: Some(StdinSource::new(reader)),
            sink: Some(Arc::new(move |_, bytes| {
                let ready = Arc::clone(&sink_ready);
                Box::pin(async move { if bytes.windows(3).any(|w| w == b"ACK") { ready.notify_one(); } Ok(()) })
            })), ..ExecControls::default()
        });
        let send = async {
            writer.write_all(b"PING").await?;
            time::timeout(Duration::from_secs(45), ready.notified()).await?;
            writer.write_all(b"tail").await?;
            writer.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        };
        let (result, sent) = tokio::join!(exec, send);
        sent?;
        ensure!(result?.result.success());

        let (_writer, reader) = io::duplex(64);
        let result = sandbox.exec().run_streaming(
            &ExecSpec::new("true").timeout(Duration::from_secs(20)),
            ExecControls { stdin: Some(StdinSource::new(reader)), ..ExecControls::default() }
        ).await?;
        ensure!(result.result.success(), "early exit failed");

        let (_writer, reader) = io::duplex(64);
        let result = sandbox.exec().run_streaming(
            &ExecSpec::new("cat").timeout(Duration::from_secs(2)),
            ExecControls { stdin: Some(StdinSource::new(reader)), ..ExecControls::default() }
        ).await?;
        ensure!(result.result.termination == Termination::TimedOut);
        let (_writer, reader) = io::duplex(64);
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        let command = ExecSpec::new("cat").timeout(Duration::from_secs(30));
        let exec = sandbox.exec().run_streaming(&command, ExecControls {
            stdin: Some(StdinSource::new(reader)), term: Some(cancel), ..ExecControls::default()
        });
        let stop = async { time::sleep(Duration::from_secs(2)).await; signal.cancel(); };
        let (result, ()) = tokio::join!(exec, stop);
        ensure!(result?.result.termination == Termination::Cancelled);
        Ok::<_, anyhow::Error>(())
    }.await;
    let cleanup = sandbox.delete().await;
    cleanup?;
    outcome
}
