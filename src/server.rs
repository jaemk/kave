use crate::error::Result;
use crate::get_config;
use std::sync::Arc;
use tokio::io::{copy, split, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

/// Server to handle client requests
struct ClientServer {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
}
impl ClientServer {
    fn new(
        svr_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
        certs: Vec<Certificate>,
        keys: Vec<PrivateKey>,
    ) -> Self {
        Self {
            svr_shutdown_send,
            sig_shutdown_recv,
            certs,
            keys,
        }
    }

    async fn _start(
        sig_shutdown_recv: UnboundedReceiver<bool>,
        certs: Vec<Certificate>,
        mut keys: Vec<PrivateKey>,
    ) -> Result<()> {
        let mut external_sig_shutdown = sig_shutdown_recv;

        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))
            .map_err(|err| format!("tls config error: {err}"))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let addr = get_config().get_client_addr();
        tracing::info!("listening for client requests on {addr}");
        let listener = TcpListener::bind(&addr).await?;

        loop {
            tokio::select! {
                _ = external_sig_shutdown.recv() => {
                    tracing::info!("client-server received sigint shutdown signal");
                    break;
                },
                // todo: clean this up... move to separate function, don't panic, and handle
                //       disconnects
                stream_peer_addr_res = listener.accept() => {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let (stream, peer_addr) = stream_peer_addr_res.expect("error accepting tls, todo don't panic");
                        let stream = acceptor.accept(stream).await.expect("error accepting stream, todo don't panic");
                        let (mut reader, mut writer) = split(stream);
                        let n = copy(&mut reader, &mut writer).await.expect("error echoing, todo don't panic");
                        writer.flush().await.expect("error flusing, todo don't panic");
                        println!("Echo: {} - {}", peer_addr, n);
                    });
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::debug!("client-server slept 500ms...");
                },
            }
        }
        Ok(())
    }

    async fn start(self) {
        tracing::info!("starting client-server");
        if let Err(e) = Self::_start(self.sig_shutdown_recv, self.certs, self.keys).await {
            tracing::error!("error starting client-server: {e}");
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
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
}

impl Server {
    pub fn new(
        svr_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
        certs: Vec<Certificate>,
        keys: Vec<PrivateKey>,
    ) -> Self {
        Self {
            svr_shutdown_send,
            sig_shutdown_recv,
            certs,
            keys,
        }
    }
    pub async fn start(self) {
        tracing::info!("starting server");
        let mut external_sig_shutdown = self.sig_shutdown_recv;

        let (client_svr_shutdown_send, mut client_svr_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();
        let (sig_client_shutdown_send, sig_client_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();

        let client_svr = ClientServer::new(
            client_svr_shutdown_send,
            sig_client_shutdown_recv,
            self.certs.clone(),
            self.keys.clone(),
        );
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
                    tracing::debug!("server slept 500ms...");
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
