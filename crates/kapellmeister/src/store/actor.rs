#![allow(clippy::result_large_err)]

use crate::{
    dataset::{Dataset, DatasetError},
    manifest::{LoadedManifest, ManifestError},
};
use display_error_chain::ErrorChainExt as _;
use flume::{Receiver, Sender};
use kapella_store::{ItemRecord, ItemStatus, JobRecord, NewJob, StatusCounts, Store, StoreError};
use std::{future::Future, io, path::PathBuf};
use thiserror::Error;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[derive(Debug, Error)]
pub enum JobError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Dataset(#[from] DatasetError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("an active job already exists; finish it or wait")]
    ActiveJobExists,
    #[error("no active job with id '{id}'")]
    NoActiveJob { id: String },
    #[error("cancelled")]
    Cancelled,
}

fn map_dataset(result: Result<Dataset, DatasetError>) -> Result<Dataset, JobError> {
    match result {
        Ok(dataset) => Ok(dataset),
        Err(DatasetError::Cancelled) => Err(JobError::Cancelled),
        Err(error) => Err(JobError::Dataset(error)),
    }
}

async fn until_cancel<T, E>(
    cancel: CancellationToken,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, JobError>
where
    JobError: From<E>,
{
    select! {
        _ = cancel.cancelled() => Err(JobError::Cancelled),
        result = fut => Ok(result?),
    }
}

#[derive(Debug)]
pub enum StoreCmd {
    StartJob {
        corr: u64,
        job_dir: PathBuf,
    },
    AddPatterns {
        corr: u64,
        id: String,
        patterns: Vec<String>,
    },
    Status {
        corr: u64,
    },
}

#[derive(Debug)]
pub enum StoreReply {
    StartJob {
        corr: u64,
        result: Result<String, JobError>,
    },
    AddPatterns {
        corr: u64,
        result: Result<String, JobError>,
    },
    Status {
        corr: u64,
        result: Result<StatusCounts, StoreError>,
    },
}

impl StoreReply {
    pub fn corr(&self) -> u64 {
        match self {
            Self::StartJob { corr, .. }
            | Self::AddPatterns { corr, .. }
            | Self::Status { corr, .. } => *corr,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LeasedWork {
    pub job: JobRecord,
    pub item: ItemRecord,
}

#[derive(Debug)]
pub enum ItemResult {
    Complete {
        item_id: i64,
        status: ItemStatus,
        exit_code: i32,
        stderr: String,
        result_dir: Option<PathBuf>,
    },
    Requeue {
        item_id: i64,
    },
}

pub async fn run(
    mut store: Store,
    cmd_rx: Receiver<StoreCmd>,
    reply_tx: Sender<StoreReply>,
    work_tx: Sender<LeasedWork>,
    result_rx: Receiver<ItemResult>,
    cancel: CancellationToken,
) {
    let mut outbound: Option<LeasedWork> = None;

    loop {
        if outbound.is_none() {
            match store.lease_next().await {
                Ok(Some((job, item))) => {
                    outbound = Some(LeasedWork { job, item });
                }
                Ok(None) => {}
                Err(error) => {
                    error!(error = %error.chain(), "lease_next failed");
                }
            }
        }

        select! {
            _ = cancel.cancelled() => {
                if let Some(work) = outbound.take() {
                    let _ = store.requeue_item(work.item.id).await;
                }
                break;
            }
            cmd = cmd_rx.recv_async() => {
                let Ok(cmd) = cmd else { break };
                handle_cmd(&mut store, &reply_tx, cmd, cancel.child_token()).await;
            }
            result = result_rx.recv_async() => {
                let Ok(result) = result else { break };
                if let Err(error) = handle_result(&mut store, result).await {
                    error!(error = %error.chain(), "failed to apply item result");
                }
            }
            send = send_outbound(&work_tx, outbound.as_ref()), if outbound.is_some() => {
                match send {
                    Ok(()) => outbound = None,
                    Err(_) => break,
                }
            }
        }
    }
}

async fn send_outbound(
    work_tx: &Sender<LeasedWork>,
    outbound: Option<&LeasedWork>,
) -> Result<(), flume::SendError<LeasedWork>> {
    let work = outbound
        .expect("send_outbound called without outbound")
        .clone();
    work_tx.send_async(work).await
}

async fn handle_cmd(
    store: &mut Store,
    reply_tx: &Sender<StoreReply>,
    cmd: StoreCmd,
    cancel: CancellationToken,
) {
    match cmd {
        StoreCmd::StartJob { corr, job_dir } => {
            let result = start_job(store, job_dir, cancel.child_token()).await;
            _ = reply_tx
                .send_async(StoreReply::StartJob { corr, result })
                .await;
        }
        StoreCmd::AddPatterns { corr, id, patterns } => {
            let result = add_patterns(store, id, patterns, cancel.child_token()).await;
            _ = reply_tx
                .send_async(StoreReply::AddPatterns { corr, result })
                .await;
        }
        StoreCmd::Status { corr } => {
            let result = store.status().await;
            _ = reply_tx
                .send_async(StoreReply::Status { corr, result })
                .await;
        }
    }
}

async fn handle_result(store: &mut Store, result: ItemResult) -> Result<(), StoreError> {
    match result {
        ItemResult::Complete {
            item_id,
            status,
            exit_code,
            stderr,
            result_dir,
        } => {
            store
                .complete_item(item_id, status, exit_code, &stderr, result_dir.as_deref())
                .await
        }
        ItemResult::Requeue { item_id } => store.requeue_item(item_id).await,
    }
}

async fn start_job(
    store: &mut Store,
    job_dir: PathBuf,
    cancel: CancellationToken,
) -> Result<String, JobError> {
    let loaded = until_cancel(cancel.child_token(), LoadedManifest::load(&job_dir)).await?;

    if until_cancel(cancel.child_token(), store.active_job())
        .await?
        .is_some()
    {
        return Err(JobError::ActiveJobExists);
    }

    let dataset = map_dataset(
        Dataset::from_patterns(
            &loaded.toml_dir,
            &loaded.manifest.job.dataset,
            cancel.child_token(),
        )
        .await,
    )?;
    let items = dataset.into_items();
    let item_count = items.len();

    until_cancel(
        cancel.child_token(),
        tokio::fs::create_dir_all(&loaded.results_root),
    )
    .await?;

    let job_id = until_cancel(
        cancel.child_token(),
        store.create_job(&NewJob {
            logical_id: loaded.manifest.id.clone(),
            command: loaded.manifest.job.command.clone(),
            env_id: loaded.manifest.env.id.clone(),
            env_setup_name: loaded.setup_name.clone(),
            env_setup: loaded.setup_script.clone(),
            env_fingerprint: loaded.env_fingerprint.clone(),
            env_vars: loaded.manifest.env.variables.clone(),
            outputs: loaded.manifest.job.outputs.clone(),
            toml_dir: loaded.toml_dir.clone(),
            results_root: loaded.results_root.clone(),
        }),
    )
    .await?;

    until_cancel(
        cancel.child_token(),
        store.insert_items(
            job_id,
            items.into_iter().map(|item| (item.path, item.rel_path)),
        ),
    )
    .await?;

    Ok(format!(
        "started job '{}' (id={job_id}) with {item_count} items",
        loaded.manifest.id
    ))
}

async fn add_patterns(
    store: &mut Store,
    id: String,
    patterns: Vec<String>,
    cancel: CancellationToken,
) -> Result<String, JobError> {
    let job = until_cancel(cancel.child_token(), store.active_job_by_logical_id(&id))
        .await?
        .ok_or_else(|| JobError::NoActiveJob { id: id.clone() })?;

    let dataset =
        map_dataset(Dataset::from_patterns(&job.toml_dir, &patterns, cancel.child_token()).await)?;
    let items = dataset.into_items();
    let inserted = until_cancel(
        cancel.child_token(),
        store.insert_items(
            job.id,
            items.into_iter().map(|item| (item.path, item.rel_path)),
        ),
    )
    .await?;

    Ok(format!("added {inserted} new items to job '{id}'"))
}
