use async_trait::async_trait;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{self, Duration};

use crate::error::IngestorError;

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, line: &str) -> Result<(), IngestorError>;
}

pub type DynSink = Arc<dyn OutputSink>;

fn spawn_writer<W>(writer: W) -> Sender<String>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<String>(1024);
    tokio::spawn(async move {
        let mut writer = BufWriter::new(writer);
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                line = rx.recv() => {
                    match line {
                        Some(line) => {
                            if writer.write_all(line.as_bytes()).await.is_err() { break; }
                            if writer.write_all(b"\n").await.is_err() { break; }
                        }
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    if writer.flush().await.is_err() { break; }
                }
            }
        }
        let _ = writer.flush().await;
    });
    tx
}

pub struct StdoutSink {
    tx: Sender<String>,
}

impl StdoutSink {
    pub fn new() -> Self {
        let tx = spawn_writer(tokio::io::stdout());
        Self { tx }
    }
}

#[async_trait]
impl OutputSink for StdoutSink {
    async fn send(&self, line: &str) -> Result<(), IngestorError> {
        self.tx
            .send(line.to_string())
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))
    }
}

pub struct FileSink {
    tx: Sender<String>,
}

impl FileSink {
    pub async fn new(path: &str) -> Result<Self, IngestorError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let tx = spawn_writer(file);
        Ok(Self { tx })
    }
}

#[async_trait]
impl OutputSink for FileSink {
    async fn send(&self, line: &str) -> Result<(), IngestorError> {
        self.tx
            .send(line.to_string())
            .await
            .map_err(|e| IngestorError::Other(e.to_string()))
    }
}
