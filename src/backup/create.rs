use walkdir::WalkDir;
use crate::context::Context;
use anyhow::Result;
use clap::Args;
use std::fs::{File, self};
use std::io::Read;
use std::path::PathBuf;
use log::info;
use std::collections::HashMap;
use scopeguard::guard;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io::{BufRead,BufWriter};
use super::manifest::{Manifest, ChunkRef};
use uuid::Uuid;

const ZSTD_LEVEL: i32 = 3;
const PROGRESS_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Args)]
pub struct Options {
    #[arg(long)]
    pub label: String,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    let id = Uuid::new_v4();
    let created_at = time::OffsetDateTime::now_utc();
    let mut client = postgres::Client::connect("host=localhost user=postgres", postgres::NoTls).unwrap();
    client.execute("SELECT pg_backup_start($1, fast := true);", &[&opts.label]).unwrap(); 
    let client = guard(client, |mut client| {
        client.execute("SELECT pg_backup_stop();", &[]).unwrap();
    });

    let backup_dir_path = ctx.storage.join("backups");
    if !backup_dir_path.exists() {
        fs::create_dir(&backup_dir_path)?;
    }

    let chunk_dir_path = ctx.storage.join("chunks");
    if !chunk_dir_path.exists() {
        fs::create_dir(&chunk_dir_path)?;
    }

    let mut files = HashMap::new();
    let mut small_blocks = HashMap::new();
    let mut processed = 0;
    let mut deduped = 0;
    let mut compressed = 0;
    let mut new = 0;
    let start_time = std::time::Instant::now();

    for entry in WalkDir::new(&ctx.cluster_data) {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() { 
            continue;
        }

        let mut chunks = Vec::new();
        let mut small_blocks_list = Vec::new();

        info!("processing file {:?}", entry.path());
        let file = File::open(entry.path()).unwrap();
        let mut chunker = Chunker::new(file);
        while let Some(chunk) = chunker.next_chunk() {
            info!("processing chunk of {} bytes. current avg {} MiB/s", chunk.len(), ((processed as f64)/1024.0/1024.0) / start_time.elapsed().as_secs_f64());
            processed += chunk.len();
            let hash: blake3::Hash = blake3::hash(chunk);
            chunks.push(ChunkRef(hash));

            let checksum = hex::encode(hash.as_bytes());
            let chunk_path = chunk_dir_path.join(&checksum);
            if !chunk_path.exists() {
                let chunk_data = zstd::bulk::compress(chunk, ZSTD_LEVEL).unwrap();
                compressed += chunk_data.len();
                new += chunk_data.len();
                fs::write(&chunk_path, chunk_data).unwrap();
            } else {
                deduped += chunk.len();
                compressed += fs::metadata(&chunk_path).unwrap().len() as usize;
            }

            for small_block in chunk.chunks(8*1024) {
                let hash: blake3::Hash = blake3::hash(small_block);
                small_blocks_list.push(ChunkRef(hash));
            }
        }

        let location = entry.path().strip_prefix(&ctx.cluster_data).unwrap();
        files.insert(location.to_owned(),chunks);
        small_blocks.insert(location.to_owned(), small_blocks_list);
    }

    info!("processed {} bytes, deduped {} byte, {}%, compression ratio: {:.2}, stored: {:.2}", processed, deduped, (deduped as f64) / (processed as f64) * 100.0, (processed as f64) / (compressed as f64), (new as f32)/1024.0/1024.0);

    let manifest = Manifest {
        id,
        created_at,
        label: opts.label.clone(),
        files,
        small_blocks
    };

    let manifest_data = serde_yaml::to_string(&manifest).unwrap();
    let manifest_path = backup_dir_path.join(format!("{}.manifest", opts.label));
    fs::write(&manifest_path, manifest_data).unwrap();

    drop(client);
    Ok(())
}

struct Chunker<R> { 
    reader: R,
    buffer: Vec<u8>,
    buffer_pos: usize,
}

impl<R> Chunker<R> where R: Read {
    fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: vec![0; 4 * 1024 * 1024],
            buffer_pos: 0,
        }
    }

    fn next_chunk(&mut self) -> Option<&[u8]> {
        loop {
            let read = self.reader.read(&mut self.buffer[self.buffer_pos..]).unwrap();
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
