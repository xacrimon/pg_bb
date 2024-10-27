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
    info!("ingesting WAL file at {:?}", raw_wal_path);
    let raw_wal = File::open(&raw_wal_path)?;
    let wal_data = zstd::stream::encode_all(&raw_wal, 3)?;
    info!(
        "compressed WAL from {} bytes to {} bytes, ratio: {:.2}x",
        raw_wal.metadata()?.len(),
        wal_data.len(),
        (raw_wal.metadata()?.len() as f32) / (wal_data.len() as f32),
    );
    let hash = blake3::hash(&wal_data);

    let wal_id = raw_wal_path.file_name().unwrap().to_string_lossy();
    let wal_target_path = ctx.storage.join(format!("{}.zst", wal_id));

    if wal_target_path.exists() {
        let existing_data = fs::read(&wal_target_path)?;
        let existing_hash = blake3::hash(&existing_data);

        if existing_hash == hash {
            info!(
                "WAL file already exists at {:?} with matching hash",
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
    info!("completed WAL file ingestion");
    Ok(())
}
