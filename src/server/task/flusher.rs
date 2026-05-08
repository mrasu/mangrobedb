use crate::application::flusher::service::FlushService;
use crate::domain::port::catalog::CatalogPort;
use crate::domain::port::object_store::ObjectStorePort;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct Flusher<C: CatalogPort, O: ObjectStorePort> {
    flush_service: Arc<FlushService<C, O>>,
}

impl<C, O> Flusher<C, O>
where
    C: CatalogPort + Send + Sync + 'static,
    O: ObjectStorePort + Send + Sync + 'static,
{
    pub fn new(flush_service: Arc<FlushService<C, O>>) -> Self {
        Self { flush_service }
    }

    pub fn spawn(self) -> FlusherHandle {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.flush_service.flush_interval());
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        self.flush_service.flush_once().await
                    }
                    _ = &mut shutdown_rx => {
                        return flush_before_shutdown(&self.flush_service).await;
                    }
                }
            }
        });

        FlusherHandle {
            shutdown_tx,
            join_handle,
        }
    }
}

#[derive(Debug)]
pub struct FlusherHandle {
    shutdown_tx: oneshot::Sender<()>,
    join_handle: JoinHandle<Result<(), FlusherShutdownError>>,
}

impl FlusherHandle {
    pub async fn shutdown(self) -> Result<(), FlusherShutdownError> {
        let _ = self.shutdown_tx.send(());
        self.join_handle.await?
    }
}

#[derive(Debug, Error)]
pub enum FlusherShutdownError {
    #[error("failed to join flusher task: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("timed out flushing buffered records before shutdown")]
    Timeout,
}

async fn flush_before_shutdown<C, O>(
    flush_service: &FlushService<C, O>,
) -> Result<(), FlusherShutdownError>
where
    C: CatalogPort,
    O: ObjectStorePort,
{
    timeout(SHUTDOWN_FLUSH_TIMEOUT, flush_service.flush_once())
        .await
        .map_err(|_| FlusherShutdownError::Timeout)
}
