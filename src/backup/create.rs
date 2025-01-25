use std::{
    cell::Cell,
    collections::HashMap,
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Args;
use log::info;
use scopeguard::guard;
use uuid::Uuid;
use walkdir::WalkDir;

use super::manifest::{BackupKind, ChunkRef, FileInfo, Manifest};
use crate::context::Context;

const ZSTD_LEVEL: i32 = 3;
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(5);

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

    let mut metrics = Metrics::new();
    let data = match &opts.delta {
        Some(delta_from) => {
            let delta_from_path = backup_dir_path.join(format!("{}.manifest", delta_from));
            let delta_from_data = fs::read_to_string(&delta_from_path)?;
            let delta_from: Manifest = serde_yaml::from_str(&delta_from_data)?;
            do_incremental(ctx, &mut metrics, id, &delta_from)?
        },
        None => do_full(ctx, &mut metrics)?,
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

fn do_incremental(
    ctx: &Context,
    metrics: &mut Metrics,
    id: Uuid,
    delta_from: &Manifest,
) -> Result<BackupKind> {
    let mut block_changed = block_changed(ctx, delta_from.id);
    let mut changed_files = HashMap::new();
    let bundle_path = ctx.storage.join("bundles").join(format!("{}.tar.zst", id));
    let bundle_file = File::create_new(&bundle_path)?;
    let tracked_writer = metrics.track_writer(&bundle_file);
    let mut bundle =
        tar::Builder::new(zstd::stream::Encoder::new(tracked_writer, ZSTD_LEVEL).unwrap());

    for path in target_files(ctx) {
        let path = path?;
        let stripped_path = path.as_path().strip_prefix(&ctx.cluster_data)?;

        let mut changed_blocks = HashMap::new();
        let mut small_block_index = 0;

        let file = File::open(&path)?;
        let mut chunker = Chunker::new(file);
        while let Some(chunk) = chunker.next_chunk() {
            metrics.add_read(chunk.len() as u64);

            for small_block in chunk.chunks(8 * 1024) {
                let hash = blake3::hash(small_block);

                if block_changed(stripped_path, small_block_index, hash) {
                    let mut header = tar::Header::new_old();
                    let block_path = stripped_path.join(small_block_index.to_string());

                    header.set_path(&block_path)?;
                    header.set_size(small_block.len() as u64);

                    bundle.append(&header, small_block)?;
                    changed_blocks.insert(small_block_index, ChunkRef(hash));
                } else {
                    metrics.add_deduplicated(small_block.len() as u64);
                }

                small_block_index += 1;
            }

            metrics.log_progress(false);
        }

        if !changed_blocks.is_empty() {
            changed_files.insert(stripped_path.to_owned(), changed_blocks);
        }
    }

    bundle.into_inner().unwrap().finish()?;
    bundle_file.sync_all()?;

    metrics.log_progress(true);
    Ok(BackupKind::Incremental {
        references: delta_from.id,
        changed_blocks: changed_files,
    })
}

fn do_full(ctx: &Context, metrics: &mut Metrics) -> Result<BackupKind> {
    let mut files = HashMap::new();
    for path in target_files(ctx) {
        let path = path?;

        let mut chunks = Vec::new();
        let mut blocks = Vec::new();

        let file = File::open(&path)?;
        let mut chunker = Chunker::new(file);
        while let Some(chunk) = chunker.next_chunk() {
            metrics.add_read(chunk.len() as u64);
            let hash = blake3::hash(chunk);
            chunks.push(ChunkRef(hash));

            let checksum = hex::encode(hash.as_bytes());
            let chunk_path = ctx.storage.join("chunks").join(&checksum);

            if !chunk_path.exists() {
                let chunk_data = zstd::bulk::compress(chunk, ZSTD_LEVEL).unwrap();
                let mut chunk_file = File::create_new(&chunk_path)?;
                chunk_file.write_all(&chunk_data)?;
                chunk_file.sync_all()?;
                metrics.add_written(chunk_data.len() as u64);
            } else {
                metrics.add_deduplicated(chunk.len() as u64);
            }

            for small_block in chunk.chunks(8 * 1024) {
                let hash = blake3::hash(small_block);
                blocks.push(ChunkRef(hash));
            }

            metrics.log_progress(false);
        }

        let path = path.strip_prefix(&ctx.cluster_data)?;
        files.insert(path.to_owned(), FileInfo { chunks, blocks });
    }

    metrics.log_progress(true);
    Ok(BackupKind::Full { files })
}

// TODO: error handling + avoid loading all manifests?
fn block_changed(
    ctx: &Context,
    delta_from: Uuid,
) -> impl FnMut(&Path, usize, blake3::Hash) -> bool + '_ {
    let mut manifests = HashMap::new();
    ctx.storage
        .join("backups")
        .read_dir()
        .unwrap()
        .for_each(|entry| {
            let entry = entry.unwrap();
            let manifest_path = entry.path();
            let manifest_data = fs::read_to_string(&manifest_path).unwrap();
            let manifest: Manifest = serde_yaml::from_str(&manifest_data).unwrap();
            manifests.insert(manifest.id, manifest);
        });

    move |file, index, hash| {
        let mut node = delta_from;

        loop {
            match &manifests[&node].data {
                BackupKind::Full { files } => {
                    let info = files.get(file);
                    break info.map(|info| info.blocks.get(index)).flatten()
                        != Some(&ChunkRef(hash));
                },
                BackupKind::Incremental {
                    references,
                    changed_blocks,
                } => {
                    let blocks = changed_blocks.get(file);
                    let chunk_ref = blocks.map(|blocks| blocks.get(&index)).flatten();

                    if let Some(chunk_ref) = chunk_ref {
                        break chunk_ref != &ChunkRef(hash);
                    } else {
                        node = *references;
                    }
                },
            }
        }
    }
}

// TODO: don't include unnecessary files + error handling
fn target_files(ctx: &Context) -> impl Iterator<Item = Result<PathBuf>> + '_ {
    let pred = |path: &Path| {
        let path = path.strip_prefix(&ctx.cluster_data).unwrap();
        !matches!(
            path.components()
                .nth(0)
                .map(|c| c.as_os_str().to_str().unwrap()),
            Some("pg_wal" | "current_logfiles" | "postmaster.pid")
        )
    };

    WalkDir::new(&ctx.cluster_data)
        .into_iter()
        .filter(move |entry| {
            entry
                .as_ref()
                .map(|entry| {
                    entry
                        .metadata()
                        .map(|metadata| metadata.is_file() && pred(entry.path()))
                })
                .unwrap_or(Ok(true))
                .unwrap_or(true)
        })
        .map(|entry| {
            entry
                .map(|entry| entry.path().to_owned())
                .map_err(Into::into)
        })
}

struct Metrics {
    start_time: Instant,
    last_log_time: Cell<Instant>,
    read_bytes: Cell<u64>,
    deduplicated_bytes: Cell<u64>,
    written_bytes: Cell<u64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            last_log_time: Cell::new(Instant::now()),
            read_bytes: Cell::new(0),
            deduplicated_bytes: Cell::new(0),
            written_bytes: Cell::new(0),
        }
    }

    fn add_read(&self, bytes: u64) {
        self.read_bytes.set(self.read_bytes.get() + bytes);
    }

    fn add_written(&self, bytes: u64) {
        self.written_bytes.set(self.written_bytes.get() + bytes);
    }

    fn track_writer<W>(&self, writer: W) -> TrackedWriter<W> {
        TrackedWriter {
            inner: writer,
            total_bytes: &self.written_bytes,
        }
    }

    fn add_deduplicated(&self, bytes: u64) {
        self.deduplicated_bytes
            .set(self.deduplicated_bytes.get() + bytes);
    }

    fn log_progress(&self, last: bool) {
        if last || self.last_log_time.get().elapsed() >= PROGRESS_LOG_INTERVAL {
            self.last_log_time.set(Instant::now());
            let read_bytes = self.read_bytes.get();
            let deduplicated_bytes = self.deduplicated_bytes.get();
            let written_bytes = self.written_bytes.get();
            let elapsed_secs = self.start_time.elapsed().as_secs_f32();
            let dedup_ratio = deduplicated_bytes as f32 / read_bytes as f32;
            let throughput = read_bytes as f32 / elapsed_secs / 1024.0 / 1024.0;
            info!(
                "{}read: {} MiB, dedup: {} MiB ({:.2}%), write: {} MiB, compression ratio: \
                 {:.2}x, throughput: {:.2} MiB/s",
                if !last { "progress: " } else { "" },
                read_bytes / 1024 / 1024,
                deduplicated_bytes / 1024 / 1024,
                dedup_ratio * 100.0,
                written_bytes / 1024 / 1024,
                (read_bytes - deduplicated_bytes) as f32 / written_bytes as f32,
                throughput
            );
        }
    }
}

struct TrackedWriter<'tracker, W> {
    inner: W,
    total_bytes: &'tracker Cell<u64>,
}

impl<'tracker, W> Write for TrackedWriter<'tracker, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.total_bytes.set(self.total_bytes.get() + len as u64);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
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
