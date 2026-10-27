use kapella_env::KapellaEnv;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const LOG_BYTE_CAP: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct BashCommand(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct KapellaJobId(u64);

impl KapellaJobId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KapellaJob {
    pub id: KapellaJobId,
    pub command: BashCommand,
    pub env: KapellaEnv,
    pub item_name: String,
    pub archive_len: u64,
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KapellaJobResult {
    pub id: KapellaJobId,
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub outputs_archive_len: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IpcRequest {
    StartJob { job_dir: PathBuf },
    AddPatterns {
        id: String,
        patterns: Vec<String>,
    },
    Status,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IpcResponse {
    Ok { message: String },
    Status {
        pending: u64,
        leased: u64,
        done: u64,
        failed: u64,
        active_job: Option<String>,
    },
    Err { message: String },
}

pub fn cap_log(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.truncate(LOG_BYTE_CAP);
    bytes
}

pub async fn stream_file_to<W>(writer: &mut W, path: &Path, len: u64) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let file = tokio::fs::File::open(path).await?;
    let mut limited = file.take(len);
    let copied = tokio::io::copy(&mut limited, writer).await?;
    if copied != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected to stream {len} bytes, sent {copied}"),
        ));
    }
    writer.flush().await?;
    Ok(())
}

pub async fn stream_to_file<R>(reader: &mut R, path: &Path, len: u64) -> io::Result<()>
where
    R: AsyncRead + Unpin,
{
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let file = tokio::fs::File::create(path).await?;
    let mut limited = reader.take(len);
    let mut writer = file;
    let copied = tokio::io::copy(&mut limited, &mut writer).await?;
    if copied != len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("expected to receive {len} bytes, got {copied}"),
        ));
    }
    writer.flush().await?;
    Ok(())
}
