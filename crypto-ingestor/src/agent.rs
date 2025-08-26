use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::error::IngestorError;

/// Types of events emitted by an [`Agent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Trade execution data.
    Trade,
    /// Incremental level-2 order book update.
    L2Diff,
    /// Full book snapshot.
    Snapshot,
    /// Best bid/ask ticker update.
    BookTicker,
}

#[async_trait]
pub trait Agent: Send {
    fn name(&self) -> &'static str;

    /// Return the list of event types this agent produces.
    fn event_types(&self) -> Vec<EventType>;

    /// Start the agent. Use `shutdown.changed().await` to exit cleanly.
    async fn run(
        &mut self,
        shutdown: tokio::sync::watch::Receiver<bool>,
        tx: Sender<String>,
    ) -> Result<(), IngestorError>;
}
