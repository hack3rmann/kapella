use futures::StreamExt;
use glob_async::glob;
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::fs;
use tokio::select;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum DatasetError {
    #[error(transparent)]
    Glob(#[from] glob_async::PatternError),
    #[error(transparent)]
    GlobWalk(#[from] glob_async::GlobError),
    #[error("cannot canonicalize {}", path.display())]
    Canonicalize {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetItem {
    pub path: PathBuf,
    pub rel_path: String,
}

#[derive(Debug)]
pub enum Dataset {
    Glob {
        root: PathBuf,
        pending: VecDeque<DatasetItem>,
    },
}

impl Dataset {
    pub async fn from_patterns(
        root: &Path,
        patterns: impl IntoIterator<Item = impl AsRef<str>>,
        cancel: CancellationToken,
    ) -> Result<Self, DatasetError> {
        let mut seen = HashSet::new();
        let mut pending = VecDeque::new();

        for pattern in patterns {
            if cancel.is_cancelled() {
                return Err(DatasetError::Cancelled);
            }

            let pattern = pattern.as_ref();
            let full_pattern = if Path::new(pattern).is_absolute() {
                pattern.to_string()
            } else {
                root.join(pattern).to_string_lossy().into_owned()
            };

            let mut entries = glob(&full_pattern)?;
            loop {
                let entry = select! {
                    _ = cancel.cancelled() => return Err(DatasetError::Cancelled),
                    entry = entries.next() => entry,
                };
                let Some(entry) = entry else {
                    break;
                };
                let path = entry?;
                if !fs::metadata(&path).await.map(|m| m.is_file()).unwrap_or(false) {
                    continue;
                }

                let abs = fs::canonicalize(&path).await.map_err(|source| {
                    DatasetError::Canonicalize {
                        path: path.clone(),
                        source,
                    }
                })?;

                if !seen.insert(abs.clone()) {
                    continue;
                }

                let rel_path = abs
                    .strip_prefix(root)
                    .map(|rel| rel.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| abs.to_string_lossy().into_owned());

                pending.push_back(DatasetItem {
                    path: abs,
                    rel_path,
                });
            }
        }

        Ok(Self::Glob {
            root: root.to_path_buf(),
            pending,
        })
    }

    pub fn poll_next(&mut self) -> Option<DatasetItem> {
        match self {
            Self::Glob { pending, .. } => pending.pop_front(),
        }
    }

    pub fn into_items(mut self) -> Vec<DatasetItem> {
        let mut items = Vec::new();
        while let Some(item) = self.poll_next() {
            items.push(item);
        }
        items
    }
}
