use cbor2::async_io::tokio::{read_value, write_value};
use clap::Parser;
use display_error_chain::ErrorChainExt as _;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use kapella_job_info::{KapellaJob, KapellaJobResult, cap_log, stream_file_to, stream_to_file};
use kapella_runtime_dir::{runtime_dir, staging_dir};
use std::{
    collections::HashSet,
    fs::File,
    io::{self, ErrorKind},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
};
use tar::{Archive, Builder};
use thiserror::Error;
use tokio::{
    fs, join,
    net::TcpStream,
    process::Command,
    select,
    signal::unix::SignalKind,
    task::{JoinError, JoinHandle},
    try_join,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Debug, Error)]
enum WorkerError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    CborEncode(#[from] cbor2::ser::Error),
    #[error(transparent)]
    CborDecode(#[from] cbor2::de::Error),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error("empty input archive")]
    EmptyArchive,
    #[error("env setup failed: {stderr}")]
    EnvSetup { stderr: String },
}

#[derive(Parser, Debug)]
#[command(version, about = "Kapella worker")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4700")]
    master: SocketAddr,

    #[arg(long, default_value_os_t = default_env_root())]
    env_root: PathBuf,
}

fn default_env_root() -> PathBuf {
    let mut path = dirs_fallback_cache();
    path.push("kapella");
    path.push("envs");
    path
}

fn dirs_fallback_cache() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        return PathBuf::from(xdg);
    }
    let mut home = std::env::home_dir().unwrap_or_else(|| PathBuf::from("."));
    home.push(".cache");
    home
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let cancel = CancellationToken::new();
    let main = tokio::spawn(listen(cancel.clone(), args));

    if let Err(error) = wait_main(cancel, main).await {
        error!(error = %error.chain(), "main job panicked");
    }
}

async fn listen(cancel: CancellationToken, args: Args) {
    if let Err(error) = try_join!(
        fs::create_dir_all(runtime_dir()),
        fs::create_dir_all(&args.env_root),
    ) {
        panic!("failed to create directory: {}", error.chain());
    }

    let mut socket = select! {
        _ = cancel.cancelled() => return,
        result = TcpStream::connect(args.master) => {
            result.unwrap_or_else(|error| {
                panic!(
                    "failed to connect to master {}: {}",
                    args.master,
                    error.chain()
                );
            })
        }
    };

    info!(master = %args.master, "connected");

    let mut prepared_envs = HashSet::new();

    loop {
        let job = select! {
            _ = cancel.cancelled() => break,
            result = read_value::<KapellaJob, _>(&mut socket) => result,
        };

        let job = match job {
            Ok(job) => job,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::ConnectionReset => break,
            Err(cbor2::de::Error::Io(error)) if error.kind() == ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("failed to read job: {}", error.chain()),
        };

        info!(job_id = job.id.get(), command = %job.command.0, "new request");

        if let Err(error) = tokio::fs::create_dir_all(staging_dir()).await {
            panic!("failed to create staging dir: {}", error.chain());
        }

        let input_archive = staging_dir().join(format!("mu-{}-in.tar.gz", job.id.get()));
        let output_archive = staging_dir().join(format!("mu-{}-out.tar.gz", job.id.get()));
        let workdir = staging_dir().join(format!("mu-{}-work", job.id.get()));

        if let Err(error) = stream_to_file(&mut socket, &input_archive, job.archive_len).await {
            error!(error = %error.chain(), "failed to receive archive");
            break;
        }

        let result = match run_job(
            &job,
            &args.env_root,
            &mut prepared_envs,
            &input_archive,
            &output_archive,
            &workdir,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                error!(error = %error.chain(), "job execution failed");
                KapellaJobResult {
                    id: job.id,
                    exit_code: -1,
                    stdout: Vec::new(),
                    stderr: cap_log(error.chain().to_string().into_bytes()),
                    outputs_archive_len: None,
                }
            }
        };

        let outputs_len = result.outputs_archive_len;
        let write_result = select! {
            _ = cancel.cancelled() => break,
            result = async {
                write_value(&mut socket, &result).await?;
                if let Some(len) = outputs_len {
                    stream_file_to(&mut socket, &output_archive, len).await?;
                }
                Ok::<_, WorkerError>(())
            } => result
        };

        _ = join!(
            fs::remove_file(&input_archive),
            fs::remove_file(&output_archive),
            fs::remove_dir_all(&workdir),
        );

        if let Err(error) = write_result {
            error!(error = %error.chain(), "failed to write result");
            break;
        }
    }
}

async fn run_job(
    job: &KapellaJob,
    env_root: &Path,
    prepared_envs: &mut HashSet<String>,
    input_archive: &Path,
    output_archive: &Path,
    workdir: &Path,
) -> Result<KapellaJobResult, WorkerError> {
    fs::create_dir_all(workdir).await?;

    let env_id = job.env.id.as_str().to_string();
    let env_dir = env_root.join(&env_id);
    if !prepared_envs.contains(&env_id) {
        ensure_env(&env_dir, job).await?;
        prepared_envs.insert(env_id);
    }

    let unpacked = {
        let archive = input_archive.to_path_buf();
        let dest = workdir.to_path_buf();
        tokio::task::spawn_blocking(move || unpack_tar_gz(&archive, &dest)).await??
    };

    let item_path = unpacked
        .into_iter()
        .next()
        .ok_or(WorkerError::EmptyArchive)?;

    let mut command = Command::new("bash");
    command
        .args(["-c", &job.command.0])
        .current_dir(workdir)
        .env("KAPELLA_ITEM", &item_path)
        .env("KAPELLA_WORKDIR", workdir)
        .env("KAPELLA_ENV", &env_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for (key, value) in &job.env.variables {
        command.env(key, value);
    }

    if let Ok(path) = std::env::var("PATH") {
        command.env("PATH", format!("{}:{path}", env_dir.join("bin").display()));
    }

    let output = command.output().await?;
    let exit_code = output.status.code().unwrap_or_default();
    let stdout = cap_log(output.stdout);
    let mut stderr = cap_log(output.stderr);

    if exit_code != 0 {
        return Ok(KapellaJobResult {
            id: job.id,
            exit_code,
            stdout,
            stderr,
            outputs_archive_len: None,
        });
    }

    if job.outputs.is_empty() {
        return Ok(KapellaJobResult {
            id: job.id,
            exit_code,
            stdout,
            stderr,
            outputs_archive_len: None,
        });
    }

    let pack = {
        let root = workdir.to_path_buf();
        let outputs = job.outputs.clone();
        let archive = output_archive.to_path_buf();
        tokio::task::spawn_blocking(move || pack_outputs(&root, &outputs, &archive)).await?
    };

    match pack {
        Ok(len) => Ok(KapellaJobResult {
            id: job.id,
            exit_code,
            stdout,
            stderr,
            outputs_archive_len: Some(len),
        }),
        Err(error) => {
            let mut message = error.chain().to_string().into_bytes();
            stderr.append(&mut message);
            Ok(KapellaJobResult {
                id: job.id,
                exit_code: 1,
                stdout,
                stderr: cap_log(stderr),
                outputs_archive_len: None,
            })
        }
    }
}

async fn ensure_env(env_dir: &Path, job: &KapellaJob) -> Result<(), WorkerError> {
    let marker = env_dir.join(".kapella-env-ready");
    if marker.exists() {
        return Ok(());
    }

    fs::create_dir_all(env_dir).await?;
    let setup_path = env_dir.join(&job.env.setup.name);
    fs::write(&setup_path, job.env.setup.bash.as_bytes()).await?;

    let output = Command::new("bash")
        .arg(&setup_path)
        .current_dir(env_dir)
        .envs(&job.env.variables)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        return Err(WorkerError::EnvSetup {
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    fs::write(&marker, b"ok").await?;
    info!(env = %job.env.id.as_str(), "environment ready");
    Ok(())
}

fn unpack_tar_gz(archive_path: &Path, dest: &Path) -> io::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dest)?;
    let file = File::open(archive_path)?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);
    let mut written = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let out = dest.join(&path);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        entry.unpack(&out)?;
        written.push(out);
    }
    Ok(written)
}

fn pack_outputs(root: &Path, relative_paths: &[String], archive_path: &Path) -> io::Result<u64> {
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(archive_path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);
    for rel in relative_paths {
        let path = root.join(rel);
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("declared output missing: {}", path.display()),
            ));
        }
        builder.append_path_with_name(&path, rel)?;
    }
    let encoder = builder.into_inner()?;
    encoder.finish()?;
    Ok(std::fs::metadata(archive_path)?.len())
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
