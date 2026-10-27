use crate::store::{DemuxClient, StoreCmd, StoreReply};
use cbor2::async_io::tokio::{read_value, write_value};
use defer::defer;
use display_error_chain::ErrorChainExt as _;
use flume::Sender;
use kapella_job_info::{IpcRequest, IpcResponse};
use kapella_runtime_dir::runtime_dir;
use std::io::ErrorKind;
use tokio::{
    fs, io,
    net::{UnixListener, UnixStream},
    select,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn listen(
    cancel: CancellationToken,
    cmd_tx: Sender<StoreCmd>,
    demux: DemuxClient,
) -> io::Result<()> {
    let mut set = JoinSet::<Result<(), cbor2::de::Error>>::new();

    fs::create_dir_all(runtime_dir()).await?;

    let sock_path = runtime_dir().join("ipc.sock");
    let listener = loop {
        match UnixListener::bind(&sock_path) {
            Ok(listener) => break listener,
            Err(err) if err.kind() == ErrorKind::AddrInUse => {
                warn!(path = %sock_path.display(), "removing socket file");
                fs::remove_file(&sock_path).await?;
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
                    Ok(Err(error)) => error!(error = %error.chain(), "ipc handler failed"),
                    Err(error) => error!(error = %error.chain(), "ipc task panicked"),
                }
                continue
            }
        };

        info!(?addr, "new ipc connection");

        set.spawn(handle_ipc(
            cancel.child_token(),
            stream,
            cmd_tx.clone(),
            demux.clone(),
        ));
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
    cmd_tx: Sender<StoreCmd>,
    demux: DemuxClient,
) -> Result<(), cbor2::de::Error> {
    loop {
        let request = select! {
            _ = cancel.cancelled() => return Ok(()),
            result = read_value::<IpcRequest, _>(&mut stream) => result,
        };

        let request = match request {
            Ok(request) => request,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        let response = match request {
            IpcRequest::StartJob { job_dir } => {
                let corr = demux.next_corr();
                let reply_rx = demux.register(corr).await;
                cmd_tx
                    .send_async(StoreCmd::StartJob { corr, job_dir })
                    .await
                    .map_err(|error| {
                        cbor2::de::Error::Io(io::Error::other(error.chain().to_string()))
                    })?;
                match reply_rx.recv_async().await {
                    Ok(StoreReply::StartJob {
                        result: Ok(message),
                        ..
                    }) => IpcResponse::Ok { message },
                    Ok(StoreReply::StartJob {
                        result: Err(error),
                        ..
                    }) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                    Ok(other) => IpcResponse::Err {
                        message: format!("unexpected store reply: {other:?}"),
                    },
                    Err(error) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                }
            }
            IpcRequest::AddPatterns { id, patterns } => {
                let corr = demux.next_corr();
                let reply_rx = demux.register(corr).await;
                cmd_tx
                    .send_async(StoreCmd::AddPatterns {
                        corr,
                        id,
                        patterns,
                    })
                    .await
                    .map_err(|error| {
                        cbor2::de::Error::Io(io::Error::other(error.chain().to_string()))
                    })?;
                match reply_rx.recv_async().await {
                    Ok(StoreReply::AddPatterns {
                        result: Ok(message),
                        ..
                    }) => IpcResponse::Ok { message },
                    Ok(StoreReply::AddPatterns {
                        result: Err(error),
                        ..
                    }) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                    Ok(other) => IpcResponse::Err {
                        message: format!("unexpected store reply: {other:?}"),
                    },
                    Err(error) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                }
            }
            IpcRequest::Status => {
                let corr = demux.next_corr();
                let reply_rx = demux.register(corr).await;
                cmd_tx
                    .send_async(StoreCmd::Status { corr })
                    .await
                    .map_err(|error| {
                        cbor2::de::Error::Io(io::Error::other(error.chain().to_string()))
                    })?;
                match reply_rx.recv_async().await {
                    Ok(StoreReply::Status {
                        result: Ok(status),
                        ..
                    }) => IpcResponse::Status {
                        pending: status.pending,
                        leased: status.leased,
                        done: status.done,
                        failed: status.failed,
                        active_job: status.active_job,
                    },
                    Ok(StoreReply::Status {
                        result: Err(error),
                        ..
                    }) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                    Ok(other) => IpcResponse::Err {
                        message: format!("unexpected store reply: {other:?}"),
                    },
                    Err(error) => IpcResponse::Err {
                        message: error.chain().to_string(),
                    },
                }
            }
        };

        let write = select! {
            _ = cancel.cancelled() => return Ok(()),
            result = write_value(&mut stream, &response) => result,
        };

        match write {
            Ok(()) => {}
            Err(cbor2::ser::Error::Io(error)) if error.kind() == ErrorKind::BrokenPipe => {
                return Ok(());
            }
            Err(cbor2::ser::Error::Io(error)) => return Err(cbor2::de::Error::Io(error)),
            Err(error) => {
                return Err(cbor2::de::Error::Io(io::Error::other(
                    error.chain().to_string(),
                )));
            }
        }
    }
}
