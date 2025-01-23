use crate::context::Context;
use anyhow::bail;
use anyhow::Result;
use log::info;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

use clap::Args;

#[derive(Debug, Args)]
pub(super) struct Options {
    #[arg(long)]
    pub(super) path: PathBuf,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    let raw_wal_path = ctx.cluster_data.join(&opts.path);
    info!("archiving WAL file at {:?}", raw_wal_path);
    let raw_wal_data = fs::read(&raw_wal_path)?;
    let wal_data = zstd::bulk::compress(&raw_wal_data, 3)?;
    info!(
        "compressed WAL from {} bytes to {} bytes, ratio: {:.2}x",
        raw_wal_data.len(),
        wal_data.len(),
        (raw_wal_data.len() as f32) / (wal_data.len() as f32),
    );

    let hash = blake3::hash(&raw_wal_data);
    let wal_id = raw_wal_path.file_name().unwrap().to_string_lossy();
    let wal_dir_path = ctx.storage.join("wal");
    let checksum = hex::encode(hash.as_bytes());
    let wal_target_path = wal_dir_path.join(format!("{}-{}.zst", wal_id, checksum));

    if !wal_dir_path.exists() {
        fs::create_dir(&wal_dir_path)?;
    }

    if wal_target_path.exists() {
        let existing_data = fs::read(&wal_target_path)?;
        let existing_hash = blake3::hash(&existing_data);

        if existing_hash == hash {
            info!(
                "WAL file already exists at {:?} with matching hash, skipping",
                wal_target_path
            );
            return Ok(());
        } else {
            bail!(
                "WAL file already exists at {:?} with different hash",
                wal_target_path
            );
        }
    }

    info!("writing WAL data to {:?}", wal_target_path);
    let mut target_file = File::create(&wal_target_path)?;
    target_file.write_all(&wal_data)?;
    target_file.sync_all()?;
    info!("completed WAL file archiving");
    Ok(())
}
