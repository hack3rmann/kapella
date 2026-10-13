use cbor2::async_io::tokio::write_value;
use kapella_job_info::KapellaJob;
use kapella_runtime_dir::runtime_dir;
use std::path::PathBuf;
use tokio::net::UnixSocket;

#[tokio::main]
async fn main() {
    let sock_path = runtime_dir().join("ipc.sock");
    let mut sock = UnixSocket::new_stream()
        .unwrap()
        .connect(sock_path)
        .await
        .unwrap();

    let job = KapellaJob {
        command: "echo 'Hello, World'".to_owned(),
        data_path: PathBuf::from("/tmp/dataset"),
    };

    write_value(&mut sock, &job).await.unwrap();
}
