use std::{
    path::{Path, PathBuf},
    sync::LazyLock,
};

static RUNTIME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| make_runtime_dir(std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from)));

pub fn runtime_dir() -> &'static Path {
    &RUNTIME_DIR
}

fn make_runtime_dir(xdg_runtime_dir: Option<PathBuf>) -> PathBuf {
    let mut dir = xdg_runtime_dir.unwrap_or_else(|| PathBuf::from("/run/user/1000"));
    dir.push("kapella");
    dir
}
