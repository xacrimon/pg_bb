use crate::context::Context;
use anyhow::Result;
use std::fs::{self, File};
use clap::Args;
use std::process::{Command, Stdio};

#[derive(Debug, Args)]
pub(super) struct Options {
    #[arg(long)]
    pub(super) label: String,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    let mut child = Command::new("pg_basebackup")
        .arg("-U")
        .arg("postgres")
        .arg("-D")
        .arg("-")
        .arg("-Ft")
        .arg("-c")
        .arg("fast")
        .arg("-Xn")
        .arg("-l")
        .arg(&opts.label)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let backup_stream = child.stdout.take().unwrap();

    let backup_dir_path = ctx.storage.join("backups");
    if !backup_dir_path.exists() {
        fs::create_dir(&backup_dir_path)?;
    }

    let backup_target_path = backup_dir_path.join(format!("{}.tar.zst", &opts.label));
    let target_file = File::create(&backup_target_path)?;
    zstd::stream::copy_encode(backup_stream, &target_file, 3)?;
    target_file.sync_all()?;
    Ok(())
}
