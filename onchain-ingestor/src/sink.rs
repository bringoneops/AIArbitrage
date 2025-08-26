use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[async_trait]
pub trait OutputSink: Send + Sync {
    async fn send(&self, line: &str) -> anyhow::Result<()>;
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
    async fn send(&self, line: &str) -> anyhow::Result<()> {
        let mut stdout = self.stdout.lock().await;
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        Ok(())
    }
}

pub struct KafkaSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
        let producer: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl OutputSink for KafkaSink {
    async fn send(&self, line: &str) -> anyhow::Result<()> {
        use rdkafka::producer::FutureRecord;
        self.producer
            .send(
                FutureRecord::to(&self.topic).payload(line).key(""),
                std::time::Duration::from_secs(0),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.into())
    }
}
