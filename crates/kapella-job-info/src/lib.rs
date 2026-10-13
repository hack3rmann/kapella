use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub struct BashCommand(pub String);

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct KapellaJob {
    pub command: String,
    pub data_path: PathBuf,
}
