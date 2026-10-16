use cbor2::async_io::tokio::write_value;
use clap::{Parser, Subcommand};
use kapella_job_info::BashCommand;
use kapella_runtime_dir::runtime_dir;
use tokio::net::UnixSocket;

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut ipc = UnixSocket::new_stream()
        .unwrap()
        .connect(runtime_dir().join("ipc.sock"))
        .await
        .unwrap();

    match args.command {
        ArgCommand::Execute { command } => {
            write_value(&mut ipc, &BashCommand(command)).await.unwrap();
        }
    }
}

/// Distributed computation orchestrator
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: ArgCommand,
}

#[derive(Subcommand, Debug)]
#[command(version, about, long_about = None)]
enum ArgCommand {
    /// Execute this string as a Bash command on a single worker
    Execute { command: String },
}
