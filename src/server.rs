use crate::error::Result;
use crate::get_config;
use crate::store::{Operation, Store, Transaction};
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

macro_rules! write_stream {
    ($id:expr, $writer:expr, $buf:expr, $addr:expr) => {
        $writer
            .write_all($buf)
            .await
            .map_err(|e| format!("session={id} error writing to socket: {e}", id = $id))?;
        $writer
            .flush()
            .await
            .map_err(|e| format!("session={id} error flusing stream: {e}", id = $id))?;
        tracing::debug!(
            session = %$id,
            "flushed {n} bytes to {peer_addr:?}",
            n = $buf.len(),
            peer_addr = $addr
        );
    };
}

pub struct Connection<S> {
    id: String,
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    acceptor: TlsAcceptor,
    store: S,
}
impl<S: Store + Send + Sync + Clone + 'static> Connection<S> {
    pub fn new(
        id: String,
        stream: tokio::net::TcpStream,
        addr: std::net::SocketAddr,
        acceptor: TlsAcceptor,
        store: S,
    ) -> Self {
        Self {
            id,
            stream,
            addr,
            acceptor,
            store,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        let id = self.id;
        let stream = self
            .acceptor
            .accept(self.stream)
            .await
            .map_err(|e| format!("session={id} error accepting stream: {e}"))?;

        let (mut reader, mut writer) = split(stream);
        let mut buf = Vec::with_capacity(1024);
        'read: loop {
            buf.clear();
            let n = match reader.read_buf(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    use std::io::ErrorKind::*;
                    match e.kind() {
                        UnexpectedEof => {
                            tracing::debug!(session = %id, "EOF on socket, disconnecting");
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
                session = %id,
                "CLIENT_MESSAGE:::<{:?}>",
                std::str::from_utf8(&buf).unwrap_or("unable to decode, invalid utf")
            );

            // TODO: Move protocol parsing to a separate type, and come up with a better format.
            //       This is just enough to test sending commands from a client to a basic store.

            // GET:3:KEY
            // SET:3:KEY:5:VALUE
            if buf.len() < 3 {
                // pass
            } else if &buf[0..3] == b"GET" {
                tracing::info!("GET");
                let mut i = 4;
                let mut size = Vec::with_capacity(16);
                while i < buf.len() && buf[i] != b':' {
                    size.push(buf[i]);
                    i += 1;
                }
                let size = match std::str::from_utf8(&size)
                    .unwrap_or("unable to decode key len")
                    .parse::<usize>()
                {
                    Ok(size) => size,
                    Err(e) => {
                        let s = format!("invalid key size: {e}\n");
                        write_stream!(id, writer, s.as_bytes(), self.addr);
                        continue 'read;
                    }
                };

                i += 1;
                let mut key = Vec::with_capacity(size);
                while key.len() < size && i < buf.len() {
                    key.push(buf[i]);
                    i += 1;
                }
                let key = std::str::from_utf8(&key)
                    .map_err(|e| format!("key {:?} is invalid utf: {e}", key))?;

                tracing::info!("GET {key:?}");
                let val = self.store.get(key).await.unwrap();
                if let Some(mut val) = val {
                    let prefix = format!("{}:", val.len());
                    let mut prefix = prefix.as_bytes().to_vec();
                    prefix.append(&mut val);
                    prefix.push(b'\n');
                    tracing::trace!("GET {key:?} {prefix:?}");
                    write_stream!(id, writer, &prefix, self.addr);
                } else {
                    tracing::trace!("GET {key:?} null");
                    write_stream!(id, writer, b"4:null\n", self.addr);
                }
                continue 'read;
            } else if &buf[0..3] == b"SET" {
                tracing::info!("SET");
                let mut i = 4;
                let mut size = Vec::with_capacity(16);
                while i < buf.len() && buf[i] != b':' {
                    size.push(buf[i]);
                    i += 1;
                }
                let size = match std::str::from_utf8(&size)
                    .unwrap_or("unable to decode key len")
                    .parse::<usize>()
                {
                    Ok(size) => size,
                    Err(e) => {
                        let s = format!("invalid key size: {e}\n");
                        write_stream!(id, writer, s.as_bytes(), self.addr);
                        continue 'read;
                    }
                };

                i += 1;
                let mut key = Vec::with_capacity(size);
                while key.len() < size && i < buf.len() {
                    key.push(buf[i]);
                    i += 1;
                }
                let key = std::str::from_utf8(&key)
                    .map_err(|e| format!("key {:?} is invalid utf: {e}", key))?;

                i += 1;
                let mut size = Vec::with_capacity(16);
                while i < buf.len() && buf[i] != b':' {
                    size.push(buf[i]);
                    i += 1;
                }
                let size = match std::str::from_utf8(&size)
                    .unwrap_or("unable to decode key len")
                    .parse::<usize>()
                {
                    Ok(size) => size,
                    Err(e) => {
                        let s = format!("invalid value size: {e}\n");
                        write_stream!(id, writer, s.as_bytes(), self.addr);
                        continue 'read;
                    }
                };

                i += 1;
                let mut val = Vec::with_capacity(size);
                while val.len() < size && i < buf.len() {
                    val.push(buf[i]);
                    i += 1;
                }

                tracing::info!("SET {key:?}");
                tracing::trace!("SET {key:?} {val:?}");
                self.store
                    .transact(Transaction::with_random_id(vec![Operation::set(
                        key,
                        val.as_slice(),
                    )]))
                    .await
                    .ok();
                let len_v = val.len().to_string();
                let res = format!("2:ok:{}:{}\n", len_v.len(), len_v);
                write_stream!(id, writer, res.as_bytes(), self.addr);
                continue 'read;
            }
            tracing::info!(session = %id, "no command found, echoing input");

            writer
                .write_all(&buf)
                .await
                .map_err(|e| format!("session={id} error writing to socket: {e}"))?;
            writer
                .flush()
                .await
                .map_err(|e| format!("session={id} error flusing stream: {e}"))?;
            tracing::debug!(
                session = %id,
                "flushed {n} bytes to {peer_addr:?}",
                peer_addr = self.addr
            );
        }
        Ok(())
    }
}

/// Server to handle client requests
pub struct ClientServer<S> {
    // sender for this instance to signal that it has shutdown
    svr_shutdown_send: UnboundedSender<bool>,
    // receiver for this instance to be notified it should shutdown
    sig_shutdown_recv: UnboundedReceiver<bool>,
    certs: Vec<Certificate>,
    keys: Vec<PrivateKey>,
    addr: Option<String>,
    store: S,
}
impl<S: Store + Send + Sync + Clone + 'static> ClientServer<S> {
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
            store,
        }
    }

    pub fn set_addr<A: Into<String>>(&mut self, addr: A) -> &mut Self {
        self.addr = Some(addr.into());
        self
    }

    async fn handle_conn(
        stream_peer_addr_res: std::result::Result<
            (tokio::net::TcpStream, std::net::SocketAddr),
            std::io::Error,
        >,
        acceptor: TlsAcceptor,
        store: S,
    ) -> Result<()> {
        uuid_with_ident!(id);
        tracing::info!(session = id, "client connected");
        let (stream, peer_addr) =
            stream_peer_addr_res.map_err(|e| format!("session={id} error accepting tls: {e}"))?;
        let conn = Connection::new(id.to_string(), stream, peer_addr, acceptor, store);
        conn.handle().await
    }

    async fn server_start(&mut self) -> Result<()> {
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(self.certs.clone(), self.keys[0].clone())
            .map_err(|err| format!("tls config error: {err}"))?;
        let acceptor = TlsAcceptor::from(Arc::new(config));

        let addr = self
            .addr
            .clone()
            .unwrap_or_else(|| get_config().get_client_addr());
        tracing::info!("listening for client requests on {addr}");
        let listener = TcpListener::bind(&addr).await?;

        loop {
            tokio::select! {
                _ = self.sig_shutdown_recv.recv() => {
                    tracing::info!("client-server received sigint shutdown signal");
                    break;
                },
                stream_peer_addr_res = listener.accept() => {
                    let acceptor = acceptor.clone();
                    let store = self.store.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_conn(stream_peer_addr_res, acceptor, store).await {
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

    pub async fn start(mut self) {
        tracing::info!("starting client-server");
        if let Err(e) = self.server_start().await {
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
