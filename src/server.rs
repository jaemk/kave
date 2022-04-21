use crate::error::Result;
use crate::get_config;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

pub fn load_certs<P: AsRef<Path>>(p: P) -> Result<Vec<Certificate>> {
    let certs: Vec<Certificate> =
        rustls_pemfile::certs(&mut BufReader::new(File::open(p.as_ref())?))
            .map_err(|_| format!("invalid cert file: {:?}", p.as_ref()))
            .map(|mut certs| certs.drain(..).map(Certificate).collect())?;
    Ok(certs)
}

pub fn load_keys<P: AsRef<Path>>(p: P) -> Result<Vec<PrivateKey>> {
    let keys: Vec<PrivateKey> =
        rustls_pemfile::rsa_private_keys(&mut BufReader::new(File::open(p.as_ref())?))
            .map_err(|_| format!("invalid key file: {:?}", p.as_ref()))
            .map(|mut keys| keys.drain(..).map(PrivateKey).collect())?;
    Ok(keys)
}

macro_rules! uuid_with_ident {
    ($name:ident) => {
        let mut __uuid_encode_buf = uuid::Uuid::encode_buffer();
        let $name = uuid::Uuid::new_v4();
        let $name = $name.simple().encode_lower(&mut __uuid_encode_buf);
    };
}

/// Server to handle client requests
pub struct ClientServer {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
    port: Option<u16>,
}
impl ClientServer {
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
            port: None,
        }
    }

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = Some(port);
        self
    }

    async fn _handle_conn(
        stream_peer_addr_res: std::result::Result<
            (tokio::net::TcpStream, std::net::SocketAddr),
            std::io::Error,
        >,
        acceptor: TlsAcceptor,
    ) -> Result<()> {
        uuid_with_ident!(id);
        tracing::info!(session = id, "client connected");

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
                "MESSAGE:::<{:?}>",
                std::str::from_utf8(&buf).unwrap_or_else(|_| "unable to decode, invalid utf")
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

    async fn _start(
        sig_shutdown_recv: UnboundedReceiver<bool>,
        certs: Vec<Certificate>,
        mut keys: Vec<PrivateKey>,
        port: Option<u16>,
    ) -> Result<()> {
        let mut external_sig_shutdown = sig_shutdown_recv;

        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))
            .map_err(|err| format!("tls config error: {err}"))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));

        // todo: make this smarter
        let addr = if let Some(port) = port {
            format!("localhost:{port}")
        } else {
            get_config().get_client_addr()
        };

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
                        if let Err(e) = Self::_handle_conn(stream_peer_addr_res, acceptor).await {
                            tracing::error!("error handling client connection {e}");
                        }
                    });
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                    tracing::trace!("client-server slept 500ms...");
                },
            }
        }
        Ok(())
    }

    pub async fn start(self) {
        tracing::info!("starting client-server");
        if let Err(e) = Self::_start(self.sig_shutdown_recv, self.certs, self.keys, self.port).await
        {
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

        let server_initiated_shutdown = loop {
            tokio::select! {
                _ = external_sig_shutdown.recv() => {
                    tracing::info!("server received sigint shutdown signal");
                    sig_client_shutdown_send.send(true).expect("error propagating shutdown signal to client-server");
                    break false;
                },
                // todo: listen tcp for inter-node communication
                //       gossip and leader election?
                // _ = socket.read() => { ... }
                //
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

        if !server_initiated_shutdown {
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

        tracing::info!("server sending shutdown signal");
        self.svr_shutdown_send
            .send(true)
            .expect("error sending server shutdown signal");
    }
}
