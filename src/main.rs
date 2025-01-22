mod archive_wal;
mod context;
mod create_backup;

use anyhow::Result;
use clap::{Parser, Subcommand};
use context::Context;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    global: GlobalOptions,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, clap::Args)]
struct GlobalOptions {
    storage: PathBuf,
    cluster_data: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Command {
    ArchiveWal(archive_wal::Options),
    CreateBackup(create_backup::Options),
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let context = Context::new(args.global.storage, args.global.cluster_data);

    match args.subcommand {
        Command::ArchiveWal(opts) => archive_wal::run(&context, &opts)?,
        Command::CreateBackup(opts) => create_backup::run(&context, &opts)?,
    }

    Ok(())
}
