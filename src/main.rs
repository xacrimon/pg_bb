mod backup;
mod context;
mod wal_pull;
mod wal_push;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use context::Context;

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
    CreateBackup(backup::create::Options),
    WalPush(wal_push::Options),
    WalPull(wal_pull::Options),
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let context = Context::new(args.global.storage, args.global.cluster_data);

    match args.subcommand {
        Command::CreateBackup(opts) => backup::create::run(&context, &opts)?,
        Command::WalPush(opts) => wal_push::run(&context, &opts)?,
        Command::WalPull(opts) => wal_pull::run(&context, &opts)?,
    }

    Ok(())
}

// TODO: - zfs vs pg_basebackup as a backup data provider
//       - content addressable storage based differential backups
//       - proper restore features
//       - storages: local, s3, gcs, azure, wasabi, b2
//       - encryption
//       - repository format
//       - repository management
//       - config files
//       - async/batched archive/restore
//       - track wal archives needed for backup
