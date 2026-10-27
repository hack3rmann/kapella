use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct KapellaEnvId(SmallString<[u8; 16]>);

impl KapellaEnvId {
    pub fn new(value: impl Into<SmallString<[u8; 16]>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl AsRef<str> for KapellaEnvId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct KapellaEnvSetup {
    /// Script file name on the worker (from Kapella.toml `setup = "..."`).
    pub name: String,
    /// Script body executed once per worker for this env id.
    pub bash: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct KapellaEnv {
    pub id: KapellaEnvId,
    pub variables: HashMap<String, String>,
    pub setup: KapellaEnvSetup,
    /// SHA-256 hex of setup body (+ watched files). Worker rebuilds env on mismatch.
    pub fingerprint: String,
}
