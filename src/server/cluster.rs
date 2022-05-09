use crate::error::Result;
use crate::get_config;
use crate::server::ClientServer;
use crate::store::Store;
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

/// Main entry point
/// Manages inter-node communication and
/// separately spawns a server to handle client requests
pub struct Server<S> {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
    addr: Option<String>,
    client_svr_addr: Option<String>,
    start_client_server: bool,
    store: S,
}

impl<S: Store + Clone + Send + Sync + 'static> Server<S> {
    pub fn new(
        svr_shutdown_send: UnboundedSender<bool>,
        sig_shutdown_recv: UnboundedReceiver<bool>,
        certs: Vec<Certificate>,
        keys: Vec<PrivateKey>,
        store: S,
    ) -> Self {
        Self {
            svr_shutdown_send,
            sig_shutdown_recv,
            certs,
            keys,
            addr: None,
            client_svr_addr: None,
            start_client_server: true,
            store,
        }
    }

    pub fn set_addr<A: Into<String>>(&mut self, addr: A) -> &mut Self {
        self.addr = Some(addr.into());
        self
    }

    pub fn set_client_server_addr<A: Into<String>>(&mut self, addr: A) -> &mut Self {
        self.client_svr_addr = Some(addr.into());
        self
    }

    pub fn set_start_client_server(&mut self, start_client_server: bool) -> &mut Self {
        self.start_client_server = start_client_server;
        self
    }

    async fn handle_conn(
        stream_peer_addr_res: std::result::Result<
            (tokio::net::TcpStream, std::net::SocketAddr),
            std::io::Error,
        >,
        acceptor: TlsAcceptor,
    ) -> Result<()> {
        uuid_with_ident!(id);
        tracing::info!(session = id, "cluster connected");

        let (stream, peer_addr) =
            stream_peer_addr_res.map_err(|e| format!("session={id} error accepting tls: {e}"))?;
        let stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| format!("session={id} error accepting stream: {e}"))?;
        let (mut reader, mut writer) = split(stream);
        let mut buf = Vec::with_capacity(1024);
        'read: loop {
            let n = match reader.read_buf(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    use std::io::ErrorKind::*;
                    match e.kind() {
                        UnexpectedEof => {
                            tracing::debug!(session = id, "EOF on socket, disconnecting");
                            break 'read;
                        }
                        _ => {
                            return Err(
                                format!("session={id} error reading from socket: {e}").into()
                            )
                        }
                    }
                }
            };

            tracing::debug!(
                session = id,
                "CLUSTER_MESSAGE:::<{:?}>",
                std::str::from_utf8(&buf).unwrap_or("unable to decode, invalid utf")
            );
            writer
                .write_all(&buf)
                .await
                .map_err(|e| format!("session={id} error writing to socket: {e}"))?;
            writer
                .flush()
                .await
                .map_err(|e| format!("session={id} error flusing stream: {e}"))?;

            tracing::debug!(session = id, "flushed {n} bytes to {peer_addr:?}");
            buf.clear();
        }
        Ok(())
    }

    pub async fn server_start(
        &mut self,
        sig_client_shutdown_send: UnboundedSender<bool>,
        mut client_svr_shutdown_recv: UnboundedReceiver<bool>,
    ) -> Result<bool> {
        // initialize cluster server
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(self.certs.clone(), self.keys[0].clone())
            .map_err(|err| format!("tls config error: {err}"))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let addr = self
            .addr
            .clone()
            .unwrap_or_else(|| get_config().get_cluster_addr());
        tracing::info!("listening for cluster requests on {addr}");
        let listener = TcpListener::bind(&addr).await?;

        let client_server_initiated_shutdown = loop {
            tokio::select! {
                _ = self.sig_shutdown_recv.recv() => {
                    tracing::info!("server received sigint shutdown signal");
                    if self.start_client_server {
                    sig_client_shutdown_send.send(true).expect("error propagating shutdown signal to client-server");
                    }
                    break false;
                },
                stream_peer_addr_res = listener.accept() => {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_conn(stream_peer_addr_res, acceptor).await {
                            tracing::error!("error handling cluster connection {e}");
                        }
                    });
                },
                // todo: send heartbeat on schedule?
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::trace!("server slept 500ms...");
                },
                _ = client_svr_shutdown_recv.recv() => {
                    tracing::info!("client-server shutdown, also shutting down");
                    break true;
                },
            }
        };

        if !client_server_initiated_shutdown && self.start_client_server {
            tracing::info!("server shutdown initiated, waiting for client-server shutdown signal");
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
        }
        Ok(client_server_initiated_shutdown)
    }

    pub async fn start(mut self) {
        tracing::info!("starting server");

        let (client_svr_shutdown_send, client_svr_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();
        let (sig_client_shutdown_send, sig_client_shutdown_recv) =
            tokio::sync::mpsc::unbounded_channel();

        if self.start_client_server {
            let mut client_svr = ClientServer::new(
                client_svr_shutdown_send,
                sig_client_shutdown_recv,
                self.certs.clone(),
                self.keys.clone(),
                self.store.clone(),
            );
            if let Some(ref client_svr_addr) = self.client_svr_addr {
                client_svr.set_addr(client_svr_addr);
            }
            tracing::info!("spawning client-server");
            tokio::spawn(async move { client_svr.start().await });
            tracing::info!("client-server spawned");
        }

        if let Err(e) = self
            .server_start(sig_client_shutdown_send, client_svr_shutdown_recv)
            .await
        {
            tracing::error!("error starting cluster-server: {e}");
        }

        tracing::info!("server sending shutdown signal");
        self.svr_shutdown_send
            .send(true)
            .expect("error sending server shutdown signal");
    }
}
