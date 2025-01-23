use std::{
    cell::Cell,
    cmp,
    fs::{self, File},
    io,
    io::{Read, Write},
    process::{Command, Stdio},
    str,
    sync::mpsc::{self, channel, TryRecvError},
    thread,
    time,
};

use ::time::OffsetDateTime;
use anyhow::{bail, Result};
use clap::Args;
use log::{error, info};
use uuid::Uuid;

use super::manifest::Manifest;
use crate::context::Context;

const LABEL_SCAN_CHUNK_SIZE: u64 = 4096;
const MIB_UNIT_SCALE: usize = 1024 * 1024;
const ZSTD_LEVEL: i32 = 3;
const PROGRESS_LOG_INTERVAL: time::Duration = time::Duration::from_secs(5);

#[derive(Debug, Args)]
pub struct Options {
    #[arg(long)]
    pub label: String,
}

pub fn run(ctx: &Context, opts: &Options) -> Result<()> {
    let backup_time = OffsetDateTime::now_utc();
    info!("starting backup with label {}", opts.label);

    let mut child = Command::new("pg_basebackup")
        .args(["-U", "postgres"])
        .args(["-D", "-"])
        .args(["-F", "t"])
        .args(["-c", "fast"])
        .args(["-X", "none"])
        .args(["-l", &opts.label])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let (mut backup_stream, rx) = Splitter::new(child.stdout.take().unwrap());
    let (label_tx, label_rx) = channel();
    thread::spawn(move || {
        let label_search = find_wal_label(rx);
        label_tx.send(label_search).unwrap();
    });

    let mut backup_buffered = Vec::new();
    let label = loop {
        match label_rx.try_recv() {
            Ok(res) => break res?,
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Disconnected) => unreachable!(),
        }

        let mut chunk = backup_stream.by_ref().take(LABEL_SCAN_CHUNK_SIZE);
        let copied = io::copy(&mut chunk, &mut backup_buffered)?;

        if copied == 0 {
            unreachable!();
        }
    };

    hex::decode(&label).expect("invalid label");
    info!(
        "found wal label {} after scanning {} bytes",
        label,
        backup_buffered.len()
    );

    let backup_dir_path = ctx.storage.join("backups");
    if !backup_dir_path.exists() {
        fs::create_dir(&backup_dir_path)?;
    }

    let bundle_id = Uuid::new_v4();
    let backup_target_path = backup_dir_path.join(format!("{}.tar.zst", bundle_id));
    let target_file = File::create(&backup_target_path)?;
    info!("writing bundle to {:?}...", backup_target_path);

    let buffer_and_stream = backup_buffered.as_slice().chain(backup_stream.into_inner());
    let total_read_bytes = Cell::new(0);
    let total_written_bytes = Cell::new(0);

    let mut tracked_reader = TrackedHashingReader::new(buffer_and_stream, &total_read_bytes);
    let tracked_writer = TrackedWriter::new(&target_file, &total_written_bytes);

    type EncoderType<'a, 'b, 'c> = zstd::stream::Encoder<'a, TrackedWriter<'b, &'c File>>;
    let chunk_size = EncoderType::recommended_input_size();
    let mut encoder = EncoderType::new(tracked_writer, ZSTD_LEVEL)?;
    let start_time = time::Instant::now();
    let mut last_info = start_time;

    let log_stats = |last: bool| {
        let elapsed = start_time.elapsed().as_secs_f32();
        let read = total_read_bytes.get() / MIB_UNIT_SCALE;
        let written = total_written_bytes.get() / MIB_UNIT_SCALE;

        info!(
            "{header}processed {read} MiB @ {read_rate:.0} MiB/s, written {written} MiB @ \
             {written_rate:.0} MiB/s, compression ratio: {ratio:.2}x",
            header = if !last { "progress: " } else { "" },
            read_rate = read as f32 / elapsed,
            written_rate = written as f32 / elapsed,
            ratio = total_read_bytes.get() as f32 / total_written_bytes.get() as f32
        );
    };

    loop {
        let mut chunk = tracked_reader.by_ref().take(chunk_size as u64);
        let copied = io::copy(&mut chunk, &mut encoder)?;

        if copied == 0 {
            break;
        }

        if last_info.elapsed() >= PROGRESS_LOG_INTERVAL {
            log_stats(false);
            last_info = time::Instant::now();
        }
    }

    log_stats(true);
    let status = child.wait()?;
    if !status.success() {
        error!("something went wrong, cleaning up...");
        bail!("pg_basebackup failed with status: {}", status);
    }

    let manifest_target_path = backup_dir_path.join(format!("{}.yaml", bundle_id));
    let manifest = Manifest {
        id: bundle_id,
        created_at: backup_time,
        label: opts.label.clone(),
        checksum: tracked_reader.finalize(),
    };

    info!("writing manifest to {:?}", manifest_target_path);
    let serialized = serde_yaml::to_string(&manifest)?;
    fs::write(&manifest_target_path, serialized)?;

    info!("write finished, flushing...");
    encoder.finish()?;
    info!("syncing file...");
    target_file.sync_all()?;
    info!("completed backup");
    Ok(())
}

fn find_wal_label(stream: SplitReceiver) -> Result<String> {
    let mut archive = tar::Archive::new(stream);
    let mut buffer = Vec::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()?.to_str() != Some("backup_label") {
            continue;
        }

        buffer.clear();
        let len = entry.read_to_end(&mut buffer)?;
        let contents = str::from_utf8(&buffer[..len])?;

        for line in contents.lines() {
            if !line.starts_with("START WAL LOCATION") {
                continue;
            }

            let part = match line.split("file").nth(1) {
                Some(part) => part,
                None => continue,
            };

            if part.len() < 2 {
                continue;
            }

            return Ok(part[1..part.len() - 1].to_string());
        }
    }

    bail!("No backup label found")
}

struct TrackedHashingReader<'tracker, R> {
    inner: R,
    total_bytes: &'tracker Cell<usize>,
    hash_state: blake3::Hasher,
}

impl<'tracker, R> TrackedHashingReader<'tracker, R> {
    fn new(inner: R, total_bytes: &'tracker Cell<usize>) -> Self {
        Self {
            inner,
            total_bytes,
            hash_state: blake3::Hasher::new(),
        }
    }

    fn finalize(self) -> blake3::Hash {
        self.hash_state.finalize()
    }
}

impl<'tracker, R> Read for TrackedHashingReader<'tracker, R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.inner.read(buf)?;
        self.total_bytes.set(self.total_bytes.get() + len);
        self.hash_state.update(&buf[..len]);
        Ok(len)
    }
}

struct TrackedWriter<'tracker, W> {
    inner: W,
    total_bytes: &'tracker Cell<usize>,
}

impl<'tracker, W> TrackedWriter<'tracker, W> {
    fn new(inner: W, total_bytes: &'tracker Cell<usize>) -> Self {
        Self { inner, total_bytes }
    }
}

impl<'tracker, W> Write for TrackedWriter<'tracker, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.total_bytes.set(self.total_bytes.get() + len);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct Splitter<R> {
    inner: R,
    tx: mpsc::Sender<Vec<u8>>,
}

impl<R> Splitter<R> {
    fn new(inner: R) -> (Self, SplitReceiver) {
        let (tx, rx) = channel();
        (Self { inner, tx }, SplitReceiver::new(rx))
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R> Read for Splitter<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.inner.read(buf)?;
        let _ = self.tx.send(buf[..len].to_vec());
        Ok(len)
    }
}

struct SplitReceiver {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
}

impl SplitReceiver {
    fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            buf: Vec::new(),
        }
    }
}

impl Read for SplitReceiver {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.buf.is_empty() {
            match self.rx.recv() {
                Ok(data) => self.buf = data,
                Err(_) => return Ok(0),
            }
        }

        let len = cmp::min(buf.len(), self.buf.len());
        buf[..len].copy_from_slice(&self.buf[..len]);
        self.buf.drain(..len);
        Ok(len)
    }
}
