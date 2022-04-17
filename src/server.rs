use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Server to handle client requests
struct ClientServer {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
}
impl ClientServer {
    fn new(
        svr_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
    ) -> Self {
        Self {
            svr_shutdown_send,
            sig_shutdown_recv,
        }
    }

    async fn start(self) {
        tracing::info!("starting client-server");
        let mut external_sig_shutdown = self.sig_shutdown_recv;

        loop {
            tokio::select! {
                _ = external_sig_shutdown.recv() => {
                    tracing::info!("client-server received sigint shutdown signal");
                    break;
                },
                // todo: listen tcp for client communication
                // _ = socket.read() => { ... }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::info!("client-server slept 500ms...");
                },
            }
        }

        tracing::info!("client-server sending shutdown signal");
        self.svr_shutdown_send
            .send(true)
            .expect("error sending client-server shutdown signal");
    }
}

/// Main entry point
/// Manages inter-node communication and
/// separately spawns a server to handle client requests
pub struct Server {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
}

impl Server {
    pub fn new(
        svr_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
    ) -> Self {
        Self {
            svr_shutdown_send,
            sig_shutdown_recv,
        }
    }
    pub async fn start(self) {
        tracing::info!("starting server");
        let mut external_sig_shutdown = self.sig_shutdown_recv;

        let (client_svr_shutdown_send, mut client_svr_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();
        let (sig_client_shutdown_send, sig_client_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();

        let client_svr = ClientServer::new(client_svr_shutdown_send, sig_client_shutdown_recv);
        tracing::info!("spawning client-server");
        tokio::spawn(async move { client_svr.start().await });
        tracing::info!("client-server spawned");

        loop {
            tokio::select! {
                _ = external_sig_shutdown.recv() => {
                    tracing::info!("server received sigint shutdown signal");
                    sig_client_shutdown_send.send(true).expect("error propagating shutdown signal to client-server");
                    break;
                },
                // todo: listen tcp for inter-node communication
                //       gossip and leader election?
                // _ = socket.read() => { ... }
                //
                // todo: send heartbeat on schedule?
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::info!("server slept 500ms...");
                },
                _ = client_svr_shutdown_recv.recv() => {
                    tracing::info!("client-server shutdown, also shutting down");
                    break;
                },
            }
        }

        if tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client_svr_shutdown_recv.recv(),
        )
        .await
        .is_err()
        {
            tracing::error!(
                "client-server failed to shutdown within 5s timeout. continuing shutdown"
            );
        }

        tracing::info!("server sending shutdown signal");
        self.svr_shutdown_send
            .send(true)
            .expect("error sending server shutdown signal");
    }
}
