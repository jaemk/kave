use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
pub struct Server {
    app_shutdown_send: UnboundedSender<bool>,
    sig_shutdown_recv: UnboundedReceiver<bool>,
}
impl Server {
    pub fn new(
        app_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
    ) -> Self {
        Server {
            app_shutdown_send,
            sig_shutdown_recv,
        }
    }
    pub async fn start(self) {
        tracing::info!("starting server");
        let mut sig_shutdown = self.sig_shutdown_recv;
        loop {
            tokio::select! {
                _ = sig_shutdown.recv() => {
                    tracing::info!("server received sigint shutdown signal");
                    break;
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::info!("slept 500ms...");
                },
            }
        }

        tracing::debug!("server sending app shutdown signal");
        self.app_shutdown_send
            .send(true)
            .expect("error sending app shutdown signal");
    }
}
