use crate::context::Context;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use log::info;
use std::fs::{self, File};
use std::io;
use std::io::Write;
use std::path::PathBuf;

use clap::Args;

#[derive(Debug, Args)]
pub struct Options {
    #[arg(long)]
    pub path: PathBuf,

    #[arg(long)]
    pub name: String,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    info!("pulling WAL file {}...", opts.name);
    let wal_dir_path = ctx.storage.join("wal");
    let entries = fs::read_dir(&wal_dir_path)?
        .map(|res| res.map(|e| e.file_name()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    let wal_file = entries
        .iter()
        .find(|file_name| {
            let name = file_name.to_string_lossy();
            name.split("-").nth(0) == Some(&opts.name)
        })
        .ok_or_else(|| anyhow::anyhow!("WAL file not found"))?;

    let wal_str = wal_file.to_string_lossy();
    let stored_checksum = wal_str
        .trim_end_matches(".zst")
        .split("-")
        .nth(1)
        .ok_or_else(|| anyhow!("WAL file name is invalid"))?;

    let wal_data = fs::read(wal_dir_path.join(&wal_file))?;
    let raw_wal_data = zstd::bulk::decompress(&wal_data, usize::MAX)?;
    let hash = blake3::hash(&raw_wal_data);
    let checksum = hex::encode(hash.as_bytes());

    if checksum != stored_checksum {
        bail!("WAL checksum mismatch");
    }

    let dest_path = ctx.cluster_data.join(&opts.path);
    info!("restoring WAL file to {:?}", dest_path);
    let mut dest_file = File::create(&dest_path)?;
    dest_file.write_all(&raw_wal_data)?;
    dest_file.sync_all()?;
    info!("WAL file restored");
    Ok(())
}
