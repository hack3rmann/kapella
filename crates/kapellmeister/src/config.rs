use std::{
    env,
    path::{Path, PathBuf},
};

pub fn get_default_config_path() -> PathBuf {
    const KAPELLA: &str = "kapella";

    let xdg = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
    let home = env::home_dir().map(|mut home| {
        home.push(".config");
        home
    });
    let etc = Path::new("/etc");

    let paths = [xdg.as_deref(), home.as_deref(), Some(etc)];
    let mut current_path = PathBuf::new();

    for path in paths.into_iter().flatten() {
        current_path.clear();
        current_path.extend([path, Path::new(KAPELLA)]);

        if current_path.exists() && current_path.is_dir() {
            return current_path;
        }
    }

    PathBuf::from("/etc/capella")
}
