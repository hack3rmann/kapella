use cbor2::async_io::tokio::write_value;
use kapella_job_info::{BashCommand, KapellaJob, SetupStep};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::TcpSocket;

#[tokio::main]
async fn main() {
    const ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 4700));

    let mut socket = TcpSocket::new_v4().unwrap().connect(ADDR).await.unwrap();

    let job = KapellaJob {
        setup_steps: vec![SetupStep::Execute {
            command: BashCommand("echo 'Hello, World'".to_owned()),
        }],
        command: BashCommand("head -1 /dev/random".to_owned()),
    };

    write_value(&mut socket, &job).await.unwrap();
}
