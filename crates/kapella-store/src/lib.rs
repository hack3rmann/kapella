use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteRow};
use sqlx::{Connection, Row};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Sqlite(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("failed to create store dir {}", path.display())]
    CreateDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("missing job {job_id} for item {item_id}")]
    MissingJob { job_id: i64, item_id: i64 },
    #[error("unknown job status: {0}")]
    UnknownJobStatus(String),
    #[error("unknown item status: {0}")]
    UnknownItemStatus(String),
    #[error("complete_item status must be done or failed")]
    InvalidCompleteStatus,
}

pub type Result<T> = std::result::Result<T, StoreError>;

const INSERT_ITEM_SQL: &str =
    "INSERT OR IGNORE INTO items (job_id, path, rel_path, status) VALUES (?1, ?2, ?3, ?4)";
const STATUS_COUNTS_SQL: &str = "SELECT status, COUNT(*) as count FROM items GROUP BY status";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Active,
    Completed,
}

impl JobStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "completed" => Ok(Self::Completed),
            other => Err(StoreError::UnknownJobStatus(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemStatus {
    Pending,
    Leased,
    Done,
    Failed,
}

impl ItemStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Done => "done",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            other => Err(StoreError::UnknownItemStatus(other.to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewJob {
    pub logical_id: String,
    pub command: String,
    pub env_id: String,
    pub env_setup_name: String,
    pub env_setup: String,
    pub env_vars: HashMap<String, String>,
    pub outputs: Vec<String>,
    pub toml_dir: PathBuf,
    pub results_root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct JobRecord {
    pub id: i64,
    pub logical_id: String,
    pub command: String,
    pub env_id: String,
    pub env_setup_name: String,
    pub env_setup: String,
    pub env_vars: HashMap<String, String>,
    pub outputs: Vec<String>,
    pub toml_dir: PathBuf,
    pub results_root: PathBuf,
    pub status: JobStatus,
}

#[derive(Clone, Debug)]
pub struct ItemRecord {
    pub id: i64,
    pub job_id: i64,
    pub path: PathBuf,
    pub rel_path: String,
    pub status: ItemStatus,
    pub exit_code: Option<i32>,
    pub stderr: Option<String>,
    pub result_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StatusCounts {
    pub pending: u64,
    pub leased: u64,
    pub done: u64,
    pub failed: u64,
    pub active_job: Option<String>,
}

pub struct Store {
    conn: SqliteConnection,
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| StoreError::CreateDir {
                    path: parent.to_path_buf(),
                    source,
                })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);

        let conn = SqliteConnection::connect_with(&options).await?;
        let mut store = Self { conn };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&mut self) -> Result<()> {
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&mut self.conn)
            .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                logical_id TEXT NOT NULL,
                command TEXT NOT NULL,
                env_id TEXT NOT NULL,
                env_setup_name TEXT NOT NULL DEFAULT 'setup.bash',
                env_setup TEXT NOT NULL,
                env_vars_json TEXT NOT NULL,
                outputs_json TEXT NOT NULL,
                toml_dir TEXT NOT NULL,
                results_root TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            "#,
        )
        .execute(&mut self.conn)
        .await?;
        // Existing DBs created before env_setup_name: add column if missing.
        let _ = sqlx::query(
            "ALTER TABLE jobs ADD COLUMN env_setup_name TEXT NOT NULL DEFAULT 'setup.bash'",
        )
        .execute(&mut self.conn)
        .await;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL REFERENCES jobs(id),
                path TEXT NOT NULL,
                rel_path TEXT NOT NULL,
                status TEXT NOT NULL,
                exit_code INTEGER,
                stderr TEXT,
                result_dir TEXT,
                leased_at INTEGER,
                finished_at INTEGER,
                UNIQUE(job_id, path)
            );
            "#,
        )
        .execute(&mut self.conn)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS items_status_idx ON items(status);")
            .execute(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn resume_leased(&mut self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE items SET status = ?1, leased_at = NULL WHERE status = ?2",
        )
        .bind(ItemStatus::Pending.as_str())
        .bind(ItemStatus::Leased.as_str())
        .execute(&mut self.conn)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn create_job(&mut self, job: &NewJob) -> Result<i64> {
        let now = now_secs();
        let result = sqlx::query(
            r#"
            INSERT INTO jobs (
                logical_id, command, env_id, env_setup_name, env_setup, env_vars_json, outputs_json,
                toml_dir, results_root, status, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
        )
        .bind(&job.logical_id)
        .bind(&job.command)
        .bind(&job.env_id)
        .bind(&job.env_setup_name)
        .bind(&job.env_setup)
        .bind(serde_json::to_string(&job.env_vars)?)
        .bind(serde_json::to_string(&job.outputs)?)
        .bind(job.toml_dir.to_string_lossy().as_ref())
        .bind(job.results_root.to_string_lossy().as_ref())
        .bind(JobStatus::Active.as_str())
        .bind(now)
        .execute(&mut self.conn)
        .await?;
        Ok(result.last_insert_rowid())
    }

    pub async fn active_job(&mut self) -> Result<Option<JobRecord>> {
        self.query_job(
            "SELECT id, logical_id, command, env_id, env_setup_name, env_setup, env_vars_json, outputs_json, toml_dir, results_root, status
             FROM jobs WHERE status = ?1 ORDER BY id DESC LIMIT 1",
            &[JobStatus::Active.as_str()],
        )
        .await
    }

    pub async fn active_job_by_logical_id(&mut self, logical_id: &str) -> Result<Option<JobRecord>> {
        let row = sqlx::query(
            "SELECT id, logical_id, command, env_id, env_setup_name, env_setup, env_vars_json, outputs_json, toml_dir, results_root, status
             FROM jobs WHERE status = ?1 AND logical_id = ?2 ORDER BY id DESC LIMIT 1",
        )
        .bind(JobStatus::Active.as_str())
        .bind(logical_id)
        .fetch_optional(&mut self.conn)
        .await?;

        row.map(|row| job_from_row(&row)).transpose()
    }

    async fn query_job(&mut self, sql: &str, binds: &[&str]) -> Result<Option<JobRecord>> {
        let mut query = sqlx::query(sql);
        for bind in binds {
            query = query.bind(*bind);
        }
        let row = query.fetch_optional(&mut self.conn).await?;
        row.map(|row| job_from_row(&row)).transpose()
    }

    pub async fn job(&mut self, job_id: i64) -> Result<Option<JobRecord>> {
        let row = sqlx::query(
            "SELECT id, logical_id, command, env_id, env_setup_name, env_setup, env_vars_json, outputs_json, toml_dir, results_root, status
             FROM jobs WHERE id = ?1",
        )
        .bind(job_id)
        .fetch_optional(&mut self.conn)
        .await?;

        row.map(|row| job_from_row(&row)).transpose()
    }

    pub async fn insert_items(
        &mut self,
        job_id: i64,
        items: impl IntoIterator<Item = (PathBuf, String)>,
    ) -> Result<u64> {
        let mut inserted = 0u64;
        for (path, rel_path) in items {
            let result = sqlx::query(INSERT_ITEM_SQL)
                .bind(job_id)
                .bind(path.to_string_lossy().as_ref())
                .bind(&rel_path)
                .bind(ItemStatus::Pending.as_str())
                .execute(&mut self.conn)
                .await?;
            inserted += result.rows_affected();
        }
        Ok(inserted)
    }

    pub async fn lease_next(&mut self) -> Result<Option<(JobRecord, ItemRecord)>> {
        let row = sqlx::query(
            "SELECT id, job_id, path, rel_path, status FROM items WHERE status = ?1 ORDER BY id ASC LIMIT 1",
        )
        .bind(ItemStatus::Pending.as_str())
        .fetch_optional(&mut self.conn)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let mut item = ItemRecord {
            id: row.try_get(0)?,
            job_id: row.try_get(1)?,
            path: PathBuf::from(row.try_get::<String, _>(2)?),
            rel_path: row.try_get(3)?,
            status: ItemStatus::Pending,
            exit_code: None,
            stderr: None,
            result_dir: None,
        };

        let now = now_secs();
        sqlx::query(
            "UPDATE items SET status = ?1, leased_at = ?2 WHERE id = ?3 AND status = ?4",
        )
        .bind(ItemStatus::Leased.as_str())
        .bind(now)
        .bind(item.id)
        .bind(ItemStatus::Pending.as_str())
        .execute(&mut self.conn)
        .await?;
        item.status = ItemStatus::Leased;

        let Some(job) = self.job(item.job_id).await? else {
            return Err(StoreError::MissingJob {
                job_id: item.job_id,
                item_id: item.id,
            });
        };

        Ok(Some((job, item)))
    }

    pub async fn requeue_item(&mut self, item_id: i64) -> Result<()> {
        sqlx::query(
            "UPDATE items SET status = ?1, leased_at = NULL WHERE id = ?2 AND status = ?3",
        )
        .bind(ItemStatus::Pending.as_str())
        .bind(item_id)
        .bind(ItemStatus::Leased.as_str())
        .execute(&mut self.conn)
        .await?;
        Ok(())
    }

    pub async fn complete_item(
        &mut self,
        item_id: i64,
        status: ItemStatus,
        exit_code: i32,
        stderr: &str,
        result_dir: Option<&Path>,
    ) -> Result<()> {
        if !matches!(status, ItemStatus::Done | ItemStatus::Failed) {
            return Err(StoreError::InvalidCompleteStatus);
        }

        let now = now_secs();
        sqlx::query(
            r#"
            UPDATE items
            SET status = ?1, exit_code = ?2, stderr = ?3, result_dir = ?4, finished_at = ?5
            WHERE id = ?6
            "#,
        )
        .bind(status.as_str())
        .bind(exit_code)
        .bind(stderr)
        .bind(result_dir.map(|path| path.to_string_lossy().into_owned()))
        .bind(now)
        .bind(item_id)
        .execute(&mut self.conn)
        .await?;

        let job_id: Option<i64> = sqlx::query("SELECT job_id FROM items WHERE id = ?1")
            .bind(item_id)
            .fetch_optional(&mut self.conn)
            .await?
            .map(|row| row.try_get(0))
            .transpose()?;

        if let Some(job_id) = job_id {
            let remaining: i64 = sqlx::query(
                "SELECT COUNT(*) as count FROM items WHERE job_id = ?1 AND status IN (?2, ?3)",
            )
            .bind(job_id)
            .bind(ItemStatus::Pending.as_str())
            .bind(ItemStatus::Leased.as_str())
            .fetch_one(&mut self.conn)
            .await?
            .try_get("count")?;

            if remaining == 0 {
                sqlx::query("UPDATE jobs SET status = ?1 WHERE id = ?2")
                    .bind(JobStatus::Completed.as_str())
                    .bind(job_id)
                    .execute(&mut self.conn)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn status(&mut self) -> Result<StatusCounts> {
        let mut counts = StatusCounts::default();
        let rows = sqlx::query(STATUS_COUNTS_SQL)
            .fetch_all(&mut self.conn)
            .await?;

        for row in rows {
            let status: String = row.try_get("status")?;
            let count: i64 = row.try_get("count")?;
            let count = count as u64;
            match ItemStatus::parse(&status)? {
                ItemStatus::Pending => counts.pending = count,
                ItemStatus::Leased => counts.leased = count,
                ItemStatus::Done => counts.done = count,
                ItemStatus::Failed => counts.failed = count,
            }
        }

        counts.active_job = self
            .active_job()
            .await?
            .map(|job| job.logical_id);
        Ok(counts)
    }
}

fn job_from_row(row: &SqliteRow) -> Result<JobRecord> {
    let env_vars_json: String = row.try_get("env_vars_json")?;
    let outputs_json: String = row.try_get("outputs_json")?;
    let status: String = row.try_get("status")?;
    Ok(JobRecord {
        id: row.try_get("id")?,
        logical_id: row.try_get("logical_id")?,
        command: row.try_get("command")?,
        env_id: row.try_get("env_id")?,
        env_setup_name: row.try_get("env_setup_name")?,
        env_setup: row.try_get("env_setup")?,
        env_vars: serde_json::from_str(&env_vars_json)?,
        outputs: serde_json::from_str(&outputs_json)?,
        toml_dir: PathBuf::from(row.try_get::<String, _>("toml_dir")?),
        results_root: PathBuf::from(row.try_get::<String, _>("results_root")?),
        status: JobStatus::parse(&status)?,
    })
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}
