use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::error::IngestorError;

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, line: &str) -> Result<(), IngestorError>;
}

pub type DynSink = Arc<dyn OutputSink>;

pub struct StdoutSink {
    stdout: Mutex<tokio::io::Stdout>,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self {
            stdout: Mutex::new(tokio::io::stdout()),
        }
    }
}

#[async_trait]
impl OutputSink for StdoutSink {
    async fn send(&self, line: &str) -> Result<(), IngestorError> {
        let mut stdout = self.stdout.lock().await;
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        Ok(())
    }
}
