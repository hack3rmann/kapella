use crate::{
    archive::{pack_file_to_tar_gz, safe_item_key, unpack_tar_gz},
    config::DEFAULT_MUSIC_PORT,
    store::{ItemResult, LeasedWork},
};
use cbor2::async_io::tokio::{read_value, write_value};
use display_error_chain::ErrorChainExt as _;
use flume::{Receiver, Sender};
use kapella_env::{KapellaEnv, KapellaEnvId, KapellaEnvSetup};
use kapella_job_info::{
    BashCommand, KapellaJob, KapellaJobId, KapellaJobResult, stream_file_to, stream_to_file,
};
use kapella_runtime_dir::staging_dir;
use kapella_store::ItemStatus;
use std::{
    io::{self, ErrorKind},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use thiserror::Error;
use tokio::{
    fs,
    net::{TcpListener, TcpStream},
    select,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
enum MusicError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    CborEncode(#[from] cbor2::ser::Error),
    #[error(transparent)]
    CborDecode(#[from] cbor2::de::Error),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error(transparent)]
    SendResult(#[from] flume::SendError<ItemResult>),
}

pub async fn listen(
    cancel: CancellationToken,
    work_rx: Receiver<LeasedWork>,
    result_tx: Sender<ItemResult>,
) -> io::Result<()> {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DEFAULT_MUSIC_PORT));
    let mut set: JoinSet<Result<(), MusicError>> = JoinSet::new();
    let listener = TcpListener::bind(addr).await?;

    info!(address = ?addr, "listening music on");

    loop {
        let (stream, peer) = select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => result?,
            Some(result) = set.join_next() => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => error!(error = %error.chain(), "music handler failed"),
                    Err(error) => error!(error = %error.chain(), "music task panicked"),
                }
                continue;
            }
        };

        info!(?peer, "musikant connected");
        set.spawn(handle_music(
            cancel.child_token(),
            stream,
            work_rx.clone(),
            result_tx.clone(),
        ));
    }

    while let Some(result) = set.join_next().await {
        if let Err(error) = result {
            error!(error = %error.chain(), "music task panicked");
        }
    }

    Ok(())
}

async fn handle_music(
    cancel: CancellationToken,
    mut stream: TcpStream,
    work_rx: Receiver<LeasedWork>,
    result_tx: Sender<ItemResult>,
) -> Result<(), MusicError> {
    loop {
        let work = select! {
            _ = cancel.cancelled() => return Ok(()),
            work = work_rx.recv_async() => {
                match work {
                    Ok(work) => work,
                    Err(_) => return Ok(()),
                }
            }
        };

        let LeasedWork { job, item } = work;

        fs::create_dir_all(staging_dir()).await?;

        let input_archive = staging_dir().join(format!("km-{}-{}-in.tar.gz", job.id, item.id));
        let output_archive = staging_dir().join(format!("km-{}-{}-out.tar.gz", job.id, item.id));

        let archive_len = tokio::task::spawn_blocking({
            let source = item.path.clone();
            let archive = input_archive.clone();
            move || pack_file_to_tar_gz(&source, &archive)
        })
        .await??;

        let item_name = item
            .path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| item.rel_path.clone());

        let header = KapellaJob {
            id: KapellaJobId::new(item.id as u64),
            command: BashCommand(job.command.clone()),
            env: KapellaEnv {
                id: KapellaEnvId::new(job.env_id.as_str()),
                variables: job.env_vars.clone(),
                setup: KapellaEnvSetup {
                    name: job.env_setup_name.clone(),
                    bash: job.env_setup.clone(),
                },
                fingerprint: job.env_fingerprint.clone(),
            },
            item_name,
            archive_len,
            outputs: job.outputs.clone(),
        };

        let send = select! {
            _ = cancel.cancelled() => {
                result_tx
                    .send_async(ItemResult::Requeue { item_id: item.id })
                    .await?;
                return Ok(());
            }
            result = async {
                write_value(&mut stream, &header).await?;
                stream_file_to(&mut stream, &input_archive, archive_len).await?;
                Ok::<_, MusicError>(())
            } => result
        };

        let _ = fs::remove_file(&input_archive).await;

        if let Err(error) = send {
            warn!(error = %error.chain(), "failed to send job; requeue");
            result_tx
                .send_async(ItemResult::Requeue { item_id: item.id })
                .await?;
            return Ok(());
        }

        let result = select! {
            _ = cancel.cancelled() => {
                result_tx
                    .send_async(ItemResult::Requeue { item_id: item.id })
                    .await?;
                return Ok(());
            }
            result = read_value::<KapellaJobResult, _>(&mut stream) => result,
        };

        let result = match result {
            Ok(result) => result,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => {
                result_tx
                    .send_async(ItemResult::Requeue { item_id: item.id })
                    .await?;
                return Ok(());
            }
            Err(error) => {
                result_tx
                    .send_async(ItemResult::Requeue { item_id: item.id })
                    .await?;
                return Err(MusicError::from(error));
            }
        };

        let mut result_dir = None;
        let mut final_status = if result.exit_code == 0 {
            ItemStatus::Done
        } else {
            ItemStatus::Failed
        };

        if let Some(len) = result.outputs_archive_len {
            let recv = stream_to_file(&mut stream, &output_archive, len).await;
            match recv {
                Ok(()) => {
                    let dest = job.results_root.join(safe_item_key(&item.rel_path));
                    let unpack = tokio::task::spawn_blocking({
                        let archive = output_archive.clone();
                        let dest = dest.clone();
                        move || unpack_tar_gz(&archive, &dest)
                    })
                    .await;

                    match unpack {
                        Ok(Ok(_)) => result_dir = Some(dest),
                        Ok(Err(error)) => {
                            warn!(error = %error.chain(), "failed to unpack outputs");
                            final_status = ItemStatus::Failed;
                        }
                        Err(error) => {
                            warn!(error = %error.chain(), "unpack task panicked");
                            final_status = ItemStatus::Failed;
                        }
                    }
                }
                Err(error) => {
                    warn!(error = %error.chain(), "failed to receive outputs archive");
                    final_status = ItemStatus::Failed;
                }
            }
            let _ = fs::remove_file(&output_archive).await;
        } else if result.exit_code == 0 && !job.outputs.is_empty() {
            final_status = ItemStatus::Failed;
        }

        let stderr = String::from_utf8_lossy(&result.stderr).into_owned();
        result_tx
            .send_async(ItemResult::Complete {
                item_id: item.id,
                status: final_status,
                exit_code: result.exit_code,
                stderr,
                result_dir,
            })
            .await?;

        info!(
            item_id = item.id,
            status = ?final_status,
            exit_code = result.exit_code,
            "item finished"
        );
    }
}
