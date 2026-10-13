use std::{
    env,
    path::{Path, PathBuf},
    sync::LazyLock,
};

static RUNTIME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| make_runtime_dir(env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from)));

pub fn runtime_dir() -> &'static Path {
    &RUNTIME_DIR
}

fn make_runtime_dir(xdg_runtime_dir: Option<PathBuf>) -> PathBuf {
    let mut dir = xdg_runtime_dir.unwrap_or_else(|| PathBuf::from("/run/user/1000"));
    dir.push("kapella");
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_runtime_dir() {
        let dir = make_runtime_dir(Some(PathBuf::from("/run/user/1001")));
        assert_eq!(dir.as_path(), "/run/user/1001/kapella");
    }

    #[test]
    fn no_runtime_dir() {
        let dir = make_runtime_dir(None);
        assert_eq!(dir.as_path(), "/run/user/1000/kapella");
    }
}
