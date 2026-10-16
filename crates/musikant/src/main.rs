use cbor2::async_io::tokio::read_value;
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::{BashCommand, KapellaJob};
use std::{
    io::ErrorKind,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    process::Stdio,
};
use tokio::{net::TcpSocket, process::Command};
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4700));

    let mut socket = TcpSocket::new_v4().unwrap().connect(ADDR).await.unwrap();

    loop {
        let KapellaJob {
            command: BashCommand(command),
        } = match read_value(&mut socket).await {
            Ok(job) => job,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => break,
            Err(error) => panic!("failed to read value: {}", error.chain()),
        };

        let bash = Command::new("bash")
            .args(["-c", command.as_str()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let output = bash.wait_with_output().await.unwrap();
        let output = String::from_utf8_lossy_owned(output.stdout);

        info!(?output);
    }
}
