use cbor2::async_io::tokio::write_value;
use display_error_chain::ErrorChainExt as _;
use flume::Receiver;
use kapella_job_info::{BashCommand, KapellaJob};
use std::{
    io::ErrorKind,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use tokio::{
    io,
    net::{TcpListener, TcpStream},
    select,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

fn send_jobs(
    cancel: CancellationToken,
    set: &mut JoinSet<Result<(), cbor2::ser::Error>>,
) -> Receiver<KapellaJob> {
    let (jobs, result) = flume::bounded(0);

    set.spawn(async move {
        for i in 0_usize.. {
            let job = KapellaJob {
                command: BashCommand(format!("echo 'Hello from job #{i}'")),
            };

            select! {
                _ = cancel.cancelled() => break,
                result = jobs.send_async(job) => {
                    if result.is_err() {
                        break;
                    }
                }
            }
        }

        Ok(())
    });

    result
}

pub async fn listen(cancel: CancellationToken) -> io::Result<()> {
    const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4700));

    let mut set = JoinSet::<Result<(), cbor2::ser::Error>>::new();
    let listener = TcpListener::bind(ADDR).await?;

    info!(address = ?ADDR, "listening music on");

    let jobs = send_jobs(cancel.child_token(), &mut set);

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
                continue;
            }
        };

        info!(?addr, "musikant connected");

        set.spawn(handle_music(jobs.clone(), cancel.child_token(), stream));
    }

    while let Some(result) = set.join_next().await {
        if let Err(error) = result {
            error!(error = %error.chain(), "ipc task panicked");
        }
    }

    Ok(())
}

async fn handle_music(
    jobs: Receiver<KapellaJob>,
    cancel: CancellationToken,
    mut stream: TcpStream,
) -> Result<(), cbor2::ser::Error> {
    loop {
        let job = select! {
            _ = cancel.cancelled() => break,
            result = jobs.recv_async() => {
                match result {
                    Ok(job) => job,
                    Err(_) => break,
                }
            }
        };

        let result = select! {
            _ = cancel.cancelled() => return Ok(()),
            result = write_value(&mut stream, &job) => result,
        };

        match result {
            Ok(()) => {}
            Err(cbor2::ser::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}
