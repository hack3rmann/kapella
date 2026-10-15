use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use smallvec::SmallVec;
use std::num::NonZeroU32;
use url::Url;

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct BashCommand(pub String);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GitRevision {
    Tag { version: String },
    Commit { hash: String },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Flags(pub SmallVec<[SmallString<[u8; 24]>; 1]>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SetupStep {
    Execute {
        command: BashCommand,
    },
    CloneGit {
        url: Url,
        revision: GitRevision,
        #[serde(default)]
        recurse_submodules: bool,
        #[serde(default)]
        depth: Option<NonZeroU32>,
        #[serde(default)]
        flags: Flags,
    },
    SetEnv {
        name: String,
        value: String,
    },
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct KapellaJob {
    pub setup_steps: Vec<SetupStep>,
    pub command: BashCommand,
}
