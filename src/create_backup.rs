use crate::context::Context;
use anyhow::Result;
use std::fs::{self, File};
use clap::Args;
use std::process::{Command, Stdio};
use std::io::Read;
use std::sync::mpsc::{channel, self};
use std::thread;
use anyhow::bail;

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

    let (backup_stream, rx) = Offstream::new(child.stdout.take().unwrap());
    thread::spawn(move || {
        find_label(rx).unwrap();
    });

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

fn find_label(stream: FromChannel) -> Result<String> {
    let mut archive = tar::Archive::new(stream);

    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()?.to_str() == Some("backup_label") {
            let mut contents = String::new();
            entry.read_to_string(&mut contents)?;

            for line in contents.lines() {
                if line.starts_with("START WAL LOCATION") {
                    let parts: Vec<&str> = line.split("file").collect();
                    if let Some(part) = parts.get(1) {
                        println!("Found label: {}", &part[1..part.len() - 1]);
                        return Ok("".to_string());
                    }
                }
            }
        }
    }

    bail!("No backup label found")
}

struct Offstream<R> {
    inner: R,
    tx: mpsc::Sender<Vec<u8>>,
}

impl<R> Offstream<R> {
    fn new(inner: R) -> (Self, FromChannel) {
        let (tx, rx) = channel();
        (Self { inner, tx }, FromChannel::new(rx))
    }
}

impl<R> Read for Offstream<R> where R: Read {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.inner.read(buf)?;
        let _ = self.tx.send(buf[..len].to_vec());
        Ok(len)
    }
}

struct FromChannel {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
}

impl FromChannel {
    fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self { rx, buf: Vec::new() }
    }
}

impl Read for FromChannel {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buf.is_empty() {
            self.buf = self.rx.recv().unwrap();
        }

        let len = std::cmp::min(buf.len(), self.buf.len());
        buf[..len].copy_from_slice(&self.buf[..len]);
        self.buf.drain(..len);
        Ok(len)
    }
}
