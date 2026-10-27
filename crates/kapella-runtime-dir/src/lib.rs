use std::{
    env,
    path::{Path, PathBuf},
    sync::LazyLock,
};

pub fn runtime_dir() -> &'static Path {
    &RUNTIME_DIR
}

pub fn staging_dir() -> &'static Path {
    &STAGING_DIR
}

pub fn data_dir() -> &'static Path {
    &DATA_DIR
}

static RUNTIME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| make_runtime_dir(env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from)));

const RUNTIME_DIR_DEFAULT: &str = "/run/user/1000";

static DATA_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| make_data_dir(env::var_os("XDG_DATA_HOME").map(PathBuf::from)));

static STAGING_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| make_staging_dir(env::var_os("XDG_DATA_HOME").map(PathBuf::from)));

fn make_runtime_dir(xdg_runtime_dir: Option<PathBuf>) -> PathBuf {
    let mut dir = xdg_runtime_dir.unwrap_or_else(|| PathBuf::from(RUNTIME_DIR_DEFAULT));
    dir.push("kapella");
    dir
}

fn make_data_dir(xdg_data_home: Option<PathBuf>) -> PathBuf {
    let mut dir = xdg_data_home.unwrap_or_else(|| {
        let mut home = env::home_dir().unwrap_or_else(|| PathBuf::from("."));
        home.push(".local");
        home.push("share");
        home
    });
    dir.push("kapella");
    dir
}

fn make_staging_dir(xdg_runtime_dir: Option<PathBuf>) -> PathBuf {
    let mut dir = make_runtime_dir(xdg_runtime_dir);
    dir.push("staging");
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_runtime_dir() {
        let dir = make_runtime_dir(Some(PathBuf::from("/run/user/1001")));
        assert_eq!(dir.as_path(), Path::new("/run/user/1001/kapella"));
    }

    #[test]
    fn no_runtime_dir() {
        let dir = make_runtime_dir(None);
        assert_eq!(dir.as_path(), Path::new("/run/user/1000/kapella"));
    }

    #[test]
    fn staging_is_under_runtime() {
        assert_eq!(staging_dir(), runtime_dir().join("staging"));
    }

    #[test]
    fn data_dir_from_xdg() {
        let dir = make_data_dir(Some(PathBuf::from("/var/lib")));
        assert_eq!(dir.as_path(), Path::new("/var/lib/kapella"));
    }
}
