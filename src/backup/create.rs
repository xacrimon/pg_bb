use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{self, BufRead, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::Args;
use log::info;
use scopeguard::guard;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;
use walkdir::WalkDir;

use super::manifest::{BackupKind, ChunkRef, FileInfo, Manifest};
use crate::context::Context;

const ZSTD_LEVEL: i32 = 3;
const PROGRESS_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Args)]
pub struct Options {
    #[arg(long)]
    pub label: String,

    #[arg(long)]
    pub delta: Option<String>,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    let id = Uuid::new_v4();
    let created_at = time::OffsetDateTime::now_utc();
    let mut client =
        postgres::Client::connect("host=localhost user=postgres", postgres::NoTls).unwrap();

    client
        .execute("SELECT pg_backup_start($1, fast := true);", &[&opts.label])
        .unwrap();

    let client = guard(client, |mut client| {
        client.execute("SELECT pg_backup_stop();", &[]).unwrap();
    });

    let backup_dir_path = ctx.storage.join("backups");
    if !backup_dir_path.exists() {
        fs::create_dir(&backup_dir_path)?;
    }

    let bundle_dir_path = ctx.storage.join("bundles");
    if !bundle_dir_path.exists() {
        fs::create_dir(&bundle_dir_path)?;
    }

    let chunk_dir_path = ctx.storage.join("chunks");
    if !chunk_dir_path.exists() {
        fs::create_dir(&chunk_dir_path)?;
    }

    let data = match &opts.delta {
        Some(delta_from) => {
            let delta_from_path = backup_dir_path.join(format!("{}.manifest", delta_from));
            let delta_from_data = fs::read_to_string(&delta_from_path)?;
            let delta_from: Manifest = serde_yaml::from_str(&delta_from_data)?;
            do_incremental(ctx, id, &delta_from)?
        },
        None => do_full(ctx)?,
    };

    let manifest = Manifest {
        id,
        created_at,
        label: opts.label.clone(),
        data,
    };

    let manifest_data = serde_yaml::to_string(&manifest).unwrap();
    let manifest_path = backup_dir_path.join(format!("{}.manifest", opts.label));
    let mut manifest_file = File::create_new(manifest_path)?;
    manifest_file.write_all(manifest_data.as_bytes())?;
    manifest_file.sync_all()?;

    drop(client);
    Ok(())
}

fn do_incremental(ctx: &Context, id: Uuid, delta_from: &Manifest) -> Result<BackupKind> {
    let mut changed_files = HashMap::new();
    let bundle_path = ctx.storage.join("bundles").join(format!("{}.tar.zst", id));
    let bundle_file = File::create_new(&bundle_path)?;
    let mut bundle =
        tar::Builder::new(zstd::stream::Encoder::new(&bundle_file, ZSTD_LEVEL).unwrap());

    for path in target_files(ctx) {
        let path = path?;
        let path = path.strip_prefix(&ctx.cluster_data)?;

        let mut changed_blocks = HashSet::new();
        let mut small_block_index = 0;

        let file = File::open(path)?;
        let mut chunker = Chunker::new(file);
        while let Some(chunk) = chunker.next_chunk() {
            for small_block in chunk.chunks(8 * 1024) {
                let hash: blake3::Hash = blake3::hash(small_block);

                if block_changed(delta_from, path, small_block_index, hash) {
                    let mut header = tar::Header::new_old();
                    let block_path = path.join(small_block_index.to_string());

                    header.set_path(&block_path)?;
                    header.set_size(small_block.len() as u64);

                    bundle.append(&header, small_block)?;
                    changed_blocks.insert((small_block_index, ChunkRef(hash)));
                }

                small_block_index += 1;
            }
        }

        if !changed_blocks.is_empty() {
            changed_files.insert(path.to_owned(), changed_blocks);
        }
    }

    bundle.into_inner().unwrap().finish()?;
    bundle_file.sync_all()?;

    Ok(BackupKind::Incremental {
        references: delta_from.id,
        changed_blocks: changed_files,
    })
}

fn do_full(ctx: &Context) -> Result<BackupKind> {
    let mut files = HashMap::new();

    for path in target_files(ctx) {
        let path = path?;
        let path = path.strip_prefix(&ctx.cluster_data)?;

        let mut chunks = Vec::new();
        let mut blocks = Vec::new();

        let file = File::open(path)?;
        let mut chunker = Chunker::new(file);
        while let Some(chunk) = chunker.next_chunk() {
            let hash: blake3::Hash = blake3::hash(chunk);
            chunks.push(ChunkRef(hash));

            let checksum = hex::encode(hash.as_bytes());
            let chunk_path = ctx.storage.join("chunks").join(&checksum);

            if !chunk_path.exists() {
                let chunk_data = zstd::bulk::compress(chunk, ZSTD_LEVEL).unwrap();
                let mut chunk_file = File::create_new(&chunk_path)?;
                chunk_file.write_all(&chunk_data)?;
                chunk_file.sync_all()?;
            }

            for small_block in chunk.chunks(8 * 1024) {
                let hash: blake3::Hash = blake3::hash(small_block);
                blocks.push(ChunkRef(hash));
            }
        }

        files.insert(path.to_owned(), FileInfo { chunks, blocks });
    }

    Ok(BackupKind::Full { files })
}

fn block_changed(delta_from: &Manifest, file: &Path, index: usize, hash: blake3::Hash) -> bool {
    match &delta_from.data {
        BackupKind::Full { files } => {
            let info = files.get(file);
            info.map(|info| info.blocks.get(index)).flatten() != Some(&ChunkRef(hash))
        },
        BackupKind::Incremental { changed_blocks, .. } => {
            let blocks = changed_blocks.get(file);
            blocks
                .map(|blocks| blocks.contains(&(index, ChunkRef(hash))))
                .unwrap_or(false)
        },
    }
}

fn target_files(ctx: &Context) -> impl Iterator<Item = Result<PathBuf>> {
    WalkDir::new(&ctx.cluster_data)
        .into_iter()
        .filter(|entry| {
            entry
                .as_ref()
                .map(|entry| entry.metadata().map(|metadata| metadata.is_file()))
                .unwrap_or(Ok(true))
                .unwrap_or(true)
        })
        .map(|entry| {
            entry
                .map(|entry| entry.path().to_owned())
                .map_err(Into::into)
        })
}

struct Chunker<R> {
    reader: R,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl<R> Chunker<R>
where
    R: Read,
{
    fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: vec![0; 4 * 1024 * 1024],
            buffer_pos: 0,
        }
    }

    fn next_chunk(&mut self) -> Option<&[u8]> {
        loop {
            let read = self
                .reader
                .read(&mut self.buffer[self.buffer_pos..])
                .unwrap();
            if read == 0 {
                if self.buffer_pos == 0 {
                    return None;
                } else {
                    let buf = &self.buffer[..self.buffer_pos];
                    self.buffer_pos = 0;
                    return Some(buf);
                }
            }

            self.buffer_pos += read;
            if self.buffer_pos == self.buffer.len() {
                let buf = &self.buffer[..self.buffer_pos];
                self.buffer_pos = 0;
                return Some(buf);
            }
        }
    }
}
