use cbor2::async_io::tokio::{read_value, write_value};
use display_error_chain::ErrorChainExt as _;
use flume::Receiver;
use kapella_job_info::{BashCommand, KapellaJob, KapellaJobId, KapellaJobResult};
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
    commands: Receiver<BashCommand>,
) -> Receiver<KapellaJob> {
    let (jobs, result) = flume::bounded(0);

    set.spawn(async move {
        let mut next_id = u64::MAX;

        loop {
            next_id = next_id.wrapping_add(1);

            let command = select! {
                _ = cancel.cancelled() => return Ok(()),
                result = commands.recv_async() => {
                    match result {
                        Ok(command) => command,
                        Err(_) => return Ok(()),
                    }
                }
            };

            let job = KapellaJob {
                id: KapellaJobId::new(next_id),
                command,
            };

            select! {
                _ = cancel.cancelled() => return Ok(()),
                result = jobs.send_async(job) => {
                    if result.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    });

    result
}

pub async fn listen(cancel: CancellationToken, commands: Receiver<BashCommand>) -> io::Result<()> {
    const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4700));

    let mut set = JoinSet::<Result<(), cbor2::ser::Error>>::new();
    let listener = TcpListener::bind(ADDR).await?;

    info!(address = ?ADDR, "listening music on");

    let jobs = send_jobs(cancel.child_token(), &mut set, commands);

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

        let result = select! {
            _ = cancel.cancelled() => return Ok(()),
            result = read_value::<KapellaJobResult, _>(&mut stream) => result,
        };

        let result = match result {
            Ok(result) => result,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => {
                return Ok(());
            }
            Err(error) => panic!("{}", error.chain()),
        };

        let stdout = String::from_utf8_lossy_owned(result.stdout);

        info!(?stdout);
    }

    Ok(())
}
