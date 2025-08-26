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

pub struct FileSink {
    file: Mutex<tokio::fs::File>,
}

impl FileSink {
    pub async fn new(path: &str) -> Result<Self, std::io::Error> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

#[async_trait]
impl OutputSink for FileSink {
    async fn send(&self, line: &str) -> Result<(), IngestorError> {
        let mut file = self.file.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        Ok(())
    }
}

pub struct KafkaSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> Result<Self, IngestorError> {
        use rdkafka::ClientConfig;
        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|e| IngestorError::Other(e.to_string()))?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl OutputSink for KafkaSink {
    async fn send(&self, line: &str) -> Result<(), IngestorError> {
        use rdkafka::producer::FutureRecord;
        self.producer
            .send(
                FutureRecord::to(&self.topic).payload(line).key(""),
                std::time::Duration::from_secs(0),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| IngestorError::Other(e.to_string()))
    }
}
