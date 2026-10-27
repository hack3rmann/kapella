pub mod archive;
pub mod config;
pub mod dataset;
pub mod ipc;
pub mod manifest;
pub mod music;
pub mod store;

use crate::{
    config::{default_store_path, get_default_config_path},
    store::{DemuxClient, ItemResult, LeasedWork, StoreCmd},
};
use clap::Parser;
use display_error_chain::ErrorChainExt as _;
use kapella_runtime_dir::runtime_dir;
use kapella_store::Store;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use tokio::{
    io, select,
    signal::unix::SignalKind,
    task::{JoinError, JoinHandle},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use waywe_daemonize::Daemonize;

/// Distributed computation orchestrator
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_os_t = get_default_config_path())]
    pub config: PathBuf,

    #[arg(long, default_value_os_t = default_store_path())]
    pub store: PathBuf,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    // _detach();

    start();
}

#[tokio::main]
async fn start() {
    let args = Args::parse();

    let mut store = Store::open(&args.store).await.unwrap_or_else(|error| {
        panic!(
            "failed to open store {}: {}",
            args.store.display(),
            error.chain()
        );
    });
    let resumed = store.resume_leased().await.unwrap_or_else(|error| {
        panic!("failed to resume leased items: {}", error.chain());
    });
    if resumed > 0 {
        info!(resumed, "requeued leased items after restart");
    }

    let (cmd_tx, cmd_rx) = flume::bounded(32);
    let (reply_tx, reply_rx) = flume::bounded(32);
    let (work_tx, work_rx) = flume::bounded(32);
    let (result_tx, result_rx) = flume::bounded(32);
    let (register_tx, register_rx) = flume::bounded(32);

    let cancel = CancellationToken::new();
    let demux = DemuxClient::new(register_tx);

    let actor = tokio::spawn(store::run_actor(
        store,
        cmd_rx,
        reply_tx,
        work_tx,
        result_rx,
        cancel.child_token(),
    ));
    let demux_task = tokio::spawn(store::run_demux(
        reply_rx,
        register_rx,
        cancel.child_token(),
    ));
    let main = tokio::spawn(listen(cancel.clone(), cmd_tx, demux, work_rx, result_tx));

    if let Err(error) = wait_main(cancel, main).await {
        error!(error = %error.chain(), "main task failed");
    }

    let _ = actor.await;
    let _ = demux_task.await;
}

async fn listen(
    cancel: CancellationToken,
    cmd_tx: flume::Sender<StoreCmd>,
    demux: DemuxClient,
    work_rx: flume::Receiver<LeasedWork>,
    result_tx: flume::Sender<ItemResult>,
) -> io::Result<()> {
    try_join!(
        music::listen(cancel.child_token(), work_rx, result_tx),
        ipc::listen(cancel.child_token(), cmd_tx, demux),
    )
    .map(|_| ())
}

async fn wait_main<T>(cancel: CancellationToken, mut join: JoinHandle<T>) -> Result<T, JoinError> {
    let mut enable_stop_signal = true;

    loop {
        select! {
            result = &mut join => {
                info!("the daemon has finished");
                break result;
            }
            signal = signals(), if enable_stop_signal => {
                info!(?signal, "got stop signal, shutting down the daemon");

                cancel.cancel();
                enable_stop_signal = false;
            }
        }
    }
}

async fn signals() -> SignalKind {
    use tokio::signal::unix::signal;

    let mut int = signal(SignalKind::interrupt()).unwrap();
    let mut quit = signal(SignalKind::quit()).unwrap();
    let mut hup = signal(SignalKind::hangup()).unwrap();
    let mut term = signal(SignalKind::terminate()).unwrap();

    select! {
        _ = int.recv() => SignalKind::interrupt(),
        _ = quit.recv() => SignalKind::quit(),
        _ = hup.recv() => SignalKind::hangup(),
        _ = term.recv() => SignalKind::terminate(),
    }
}

fn _detach() {
    fs::create_dir_all(runtime_dir()).unwrap();

    let stdout = File::create(runtime_dir().join("stdout.log")).unwrap();
    let stderr = File::create(runtime_dir().join("stderr.log")).unwrap();

    Daemonize::default()
        .pid_file(runtime_dir().join("daemon.pid"))
        .stdout(stdout)
        .stderr(stderr)
        .start()
        .unwrap();
}
