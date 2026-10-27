use serde::Deserialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tokio::fs;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error("cannot resolve Kapella.toml {}", path.display())]
    Resolve {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Kapella.toml has no parent dir")]
    NoParentDir,
    #[error("failed to read setup script {}", path.display())]
    ReadSetup {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct KapellaManifest {
    pub id: String,
    pub env: EnvSection,
    pub job: JobSection,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EnvSection {
    pub id: String,
    pub setup: String,
    #[serde(default)]
    pub variables: HashMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct JobSection {
    pub command: String,
    pub dataset: Vec<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct LoadedManifest {
    pub manifest: KapellaManifest,
    pub toml_dir: PathBuf,
    pub toml_path: PathBuf,
    pub setup_name: String,
    pub setup_script: String,
    pub results_root: PathBuf,
}

impl LoadedManifest {
    pub async fn load(path: &Path) -> Result<Self, ManifestError> {
        let toml_path =
            fs::canonicalize(path)
                .await
                .map_err(|source| ManifestError::Resolve {
                    path: path.to_path_buf(),
                    source,
                })?;
        let toml_dir = toml_path
            .parent()
            .ok_or(ManifestError::NoParentDir)?
            .to_path_buf();

        let raw = fs::read_to_string(&toml_path).await?;
        let manifest: KapellaManifest = toml::from_str(&raw)?;

        let setup_path = if Path::new(&manifest.env.setup).is_absolute() {
            PathBuf::from(&manifest.env.setup)
        } else {
            toml_dir.join(&manifest.env.setup)
        };
        let setup_name = setup_path
            .file_name()
            .ok_or_else(|| {
                ManifestError::ReadSetup {
                    path: setup_path.clone(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "setup path has no file name",
                    ),
                }
            })?
            .to_string_lossy()
            .into_owned();
        let setup_script =
            fs::read_to_string(&setup_path)
                .await
                .map_err(|source| ManifestError::ReadSetup {
                    path: setup_path,
                    source,
                })?;

        let results_root = toml_dir.join("results");

        Ok(Self {
            manifest,
            toml_dir,
            toml_path,
            setup_name,
            setup_script,
            results_root,
        })
    }
}
