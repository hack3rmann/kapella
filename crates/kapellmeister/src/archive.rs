use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};
use tar::{Archive, Builder};

pub fn pack_file_to_tar_gz(source: &Path, archive_path: &Path) -> io::Result<u64> {
    if let Some(parent) = archive_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file = File::create(archive_path)?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);

    let name = source
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "source has no file name"))?;

    builder.append_path_with_name(source, name)?;
    let encoder = builder.into_inner()?;
    encoder.finish()?;

    Ok(fs::metadata(archive_path)?.len())
}

pub fn unpack_tar_gz(archive_path: &Path, dest: &Path) -> io::Result<Vec<PathBuf>> {
    fs::create_dir_all(dest)?;
    let file = File::open(archive_path)?;
    let decoder = GzDecoder::new(file);
    let mut archive = Archive::new(decoder);

    let mut written = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let out = dest.join(&path);
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)?;
        }
        entry.unpack(&out)?;
        written.push(out);
    }

    Ok(written)
}

pub fn safe_item_key(rel_path: &str) -> PathBuf {
    PathBuf::from(rel_path.replace('\\', "/"))
}
