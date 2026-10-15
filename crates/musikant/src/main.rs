use kapella_runtime_dir::runtime_dir;
use tokio::net::UnixSocket;

#[tokio::main]
async fn main() {
    let sock_path = runtime_dir().join("ipc.sock");
    let mut _sock = UnixSocket::new_stream()
        .unwrap()
        .connect(sock_path)
        .await
        .unwrap();
}
