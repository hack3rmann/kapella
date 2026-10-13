use cbor2::async_io::tokio::read_value;
use defer::defer;
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::KapellaJob;
use kapella_runtime_dir::runtime_dir;
use std::{
    fs::{self, File},
    io::ErrorKind,
};
use tokio::{
    io,
    net::{UnixListener, UnixStream},
    runtime::Builder as RuntimeBuilder,
    select,
    signal::unix::SignalKind,
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use waywe_daemonize::Daemonize;

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|err| panic!("failed to build tokio runtime: {}", err.chain()));

    runtime.block_on(start());
}

async fn start() {
    let cancel = CancellationToken::new();

    let main = tokio::spawn(listen_ipc(cancel.clone()));

    if let Err(error) = wait_main(cancel, main).await {
        error!(error = %error.chain(), "main task failed");
    }
}

async fn listen_ipc(cancel: CancellationToken) -> io::Result<()> {
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
