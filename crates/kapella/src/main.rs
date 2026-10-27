use cbor2::async_io::tokio::{read_value, write_value};
use clap::{Parser, Subcommand};
use display_error_chain::ErrorChainExt as _;
use kapella_job_info::{IpcRequest, IpcResponse};
use kapella_runtime_dir::runtime_dir;
use std::path::PathBuf;
use tokio::net::UnixSocket;

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let sock = runtime_dir().join("ipc.sock");
    let mut ipc = UnixSocket::new_stream()
        .unwrap_or_else(|error| {
            panic!("failed to create unix socket: {}", error.chain());
        })
        .connect(&sock)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "failed to connect to kapellmeister ipc socket ({}): {}",
                sock.display(),
                error.chain()
            );
        });

    let request = match args.command {
        ArgCommand::Start { job } => IpcRequest::StartJob { job_dir: job },
        ArgCommand::Add { id, patterns } => IpcRequest::AddPatterns { id, patterns },
        ArgCommand::Status => IpcRequest::Status,
    };

    write_value(&mut ipc, &request)
        .await
        .unwrap_or_else(|error| {
            panic!("failed to send ipc request: {}", error.chain());
        });
    let response: IpcResponse = read_value(&mut ipc).await.unwrap_or_else(|error| {
        panic!("failed to read ipc response: {}", error.chain());
    });

    match response {
        IpcResponse::Ok { message } => {
            println!("{message}");
        }
        IpcResponse::Status {
            pending,
            leased,
            done,
            failed,
            active_job,
        } => {
            let status = serde_json::json!({
                "active_job": active_job,
                "pending": pending,
                "leased": leased,
                "done": done,
                "failed": failed,
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&status).unwrap_or_else(|error| {
                    panic!("failed to serialize status json: {}", error.chain());
                })
            );
        }
        IpcResponse::Err { message } => {
            panic!("{message}");
        }
    }
}

/// Kapella client CLI
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: ArgCommand,
}

#[derive(Subcommand, Debug)]
#[command(version, about, long_about = None)]
enum ArgCommand {
    /// Start a job from a directory containing Kapella.toml
    Start {
        /// Job directory (must contain Kapella.toml)
        job: PathBuf,
    },
    /// Add glob patterns to a running job
    Add {
        /// Job id from Kapella.toml
        id: String,
        /// Glob patterns relative to the job directory
        patterns: Vec<String>,
    },
    /// Show queue status
    Status,
}
