pub mod config;
pub mod ipc;
pub mod music;

use crate::config::get_default_config_path;
use clap::Parser;
use display_error_chain::ErrorChainExt as _;
use kapella_runtime_dir::runtime_dir;
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
    let _args = Args::parse();

    let cancel = CancellationToken::new();
    let main = tokio::spawn(listen(cancel.clone()));

    if let Err(error) = wait_main(cancel, main).await {
        error!(error = %error.chain(), "main task failed");
    }
}

async fn listen(cancel: CancellationToken) -> io::Result<()> {
    try_join!(
        music::listen(cancel.child_token()),
        ipc::listen(cancel.child_token()),
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
