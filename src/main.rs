mod context;
mod ingest_wal;

use clap::{Parser, Subcommand};
use context::Context;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    IngestWal(ingest_wal::Options),
}

fn main() {
    env_logger::Builder::new()
        .write_style(env_logger::WriteStyle::Always)
        .build();
    let args = Args::parse();

    println!("Hello, world!");
}
