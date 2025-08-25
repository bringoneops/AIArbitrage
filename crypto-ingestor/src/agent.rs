use async_trait::async_trait;

#[async_trait]
pub trait Agent: Send {
    fn name(&self) -> &'static str;

    /// Start the agent. Use `shutdown.changed().await` to exit cleanly.
    async fn run(
        &mut self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
