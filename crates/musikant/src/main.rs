use cbor2::async_io::tokio::{read_value, write_value};
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::{BashCommand, KapellaJob, KapellaJobResult};
use std::{
    io::ErrorKind,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    process::Stdio,
};
use tokio::{
    net::TcpSocket,
    process::Command,
    select,
    signal::unix::SignalKind,
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let cancel = CancellationToken::new();
    let main = tokio::spawn(listen(cancel.clone()));

    if let Err(error) = wait_main(cancel, main).await {
        error!(error = %error.chain(), "main job panicked");
    }
}

async fn listen(cancel: CancellationToken) {
    const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4700));

    let mut socket = select! {
        _ = cancel.cancelled() => return,
        result = TcpSocket::new_v4().unwrap().connect(ADDR) => result.unwrap(),
    };

    loop {
        let result = select! {
            _ = cancel.cancelled() => break,
            result = read_value(&mut socket) => result,
        };

        let KapellaJob {
            command: BashCommand(command),
            id,
        } = match result {
            Ok(job) => job,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => break,
            Err(error) => panic!("failed to read value: {}", error.chain()),
        };

        info!(command, "new request");

        let bash = Command::new("bash")
            .args(["-c", command.as_str()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let output = select! {
            _ = cancel.cancelled() => break,
            result = bash.wait_with_output() => result.unwrap(),
        };

        let exit_code = output.status.code().unwrap_or_default();
        let result = KapellaJobResult {
            id,
            exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        };

        if exit_code == 0 {
            info!("exited with 0");
        } else {
            warn!(exit_code, "failed")
        }

        let result = select! {
            _ = cancel.cancelled() => break,
            result = write_value(&mut socket, &result) => result,
        };

        match result {
            Ok(()) => {}
            Err(cbor2::ser::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => {
                break;
            }
            Err(error) => panic!("failed to write value: {}", error.chain()),
        }
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
