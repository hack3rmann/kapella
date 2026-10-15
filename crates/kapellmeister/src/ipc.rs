use cbor2::async_io::tokio::read_value;
use defer::defer;
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::KapellaJob;
use kapella_runtime_dir::runtime_dir;
use std::io::ErrorKind;
use tokio::{
    io,
    net::{UnixListener, UnixStream},
    select,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn listen(cancel: CancellationToken) -> io::Result<()> {
    let mut set = JoinSet::<Result<(), cbor2::de::Error>>::new();

    let sock_path = runtime_dir().join("ipc.sock");
    let listener = loop {
        match UnixListener::bind(&sock_path) {
            Ok(listener) => break listener,
            Err(err) if err.kind() == ErrorKind::AddrInUse => {
                warn!(path = %sock_path.display(), "removing socket file");
                tokio::fs::remove_file(&sock_path).await?;
            }
            Err(error) => return Err(error),
        }
    };

    info!(path = %sock_path.display(), "listening on ipc socket");

    defer!({
        info!(path = %sock_path.display(), "removing socket file");

        if let Err(error) = std::fs::remove_file(&sock_path) {
            error!(error = %error.chain(), path = %sock_path.display(), "cannot remove socket file");
        }
    });

    loop {
        let (stream, addr) = select! {
            result = listener.accept() => result?,
            _ = cancel.cancelled() => break,
            Some(result) = set.join_next() => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => error!(error = %error.chain(), "job failed"),
                    Err(error) => error!(error = %error.chain(), "ipc task panicked"),
                }
                continue
            }
        };

        info!(?addr, "new ipc connection");

        set.spawn(handle_ipc(cancel.child_token(), stream));
    }

    while let Some(result) = set.join_next().await {
        if let Err(error) = result {
            error!(error = %error.chain(), "ipc task panicked");
        }
    }

    Ok(())
}

async fn handle_ipc(
    cancel: CancellationToken,
    mut stream: UnixStream,
) -> Result<(), cbor2::de::Error> {
    loop {
        let result = select! {
            _ = cancel.cancelled() => return Ok(()),
            result = read_value::<KapellaJob, _>(&mut stream) => result,
        };

        let job = match result {
            Ok(job) => job,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        info!(?job, "job");
    }
}
