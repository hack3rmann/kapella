use cbor2::async_io::tokio::read_value;
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::KapellaJob;
use std::io::ErrorKind;
use tokio::{
    io,
    net::{TcpListener, TcpStream},
    select,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub async fn listen(cancel: CancellationToken) -> io::Result<()> {
    const ADDR: &str = "127.0.0.1:4700";

    let mut set = JoinSet::<Result<(), cbor2::de::Error>>::new();
    let listener = TcpListener::bind(ADDR).await?;

    info!(address = ADDR, "listening music on");

    loop {
        let (stream, addr) = select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => result?,
            Some(result) = set.join_next() => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => error!(error = %error.chain(), "job failed"),
                    Err(error) => error!(error = %error.chain(), "ipc task panicked"),
                }
                continue
            }
        };

        info!(?addr, "musikant connected");

        set.spawn(handle_music(cancel.child_token(), stream));
    }

    while let Some(result) = set.join_next().await {
        if let Err(error) = result {
            error!(error = %error.chain(), "ipc task panicked");
        }
    }

    Ok(())
}

async fn handle_music(
    cancel: CancellationToken,
    mut stream: TcpStream,
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
