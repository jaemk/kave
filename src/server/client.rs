use crate::error::Result;
use crate::get_config;
use crate::proto;
use crate::store::{Operation, Store, Transaction};
use std::sync::Arc;
use tokio::io::split;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;

pub struct Connection<S> {
    id: String,
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    acceptor: TlsAcceptor,
    store: S,
    kill: Receiver<bool>,
}
impl<S: Store + Send + Sync + Clone + 'static> Connection<S> {
    pub fn new(
        id: String,
        stream: tokio::net::TcpStream,
        addr: std::net::SocketAddr,
        acceptor: TlsAcceptor,
        store: S,
        kill: Receiver<bool>,
    ) -> Self {
        Self {
            id,
            stream,
            addr,
            acceptor,
            store,
            kill,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        let id = self.id;
        let stream = self
            .acceptor
            .accept(self.stream)
            .await
            .map_err(|e| format!("session={id} error accepting stream: {e}"))?;

        let (reader, mut writer) = split(stream);
        let mut proto = proto::Proto::new(&id, self.addr, reader, self.kill);
        loop {
            let op = proto.read().await;
            match op? {
                proto::ProtoOp::SysClose => {
                    tracing::debug!(session = %id, "EOF on socket, disconnecting");
                    return Ok(());
                }
                proto::ProtoOp::Cancelled => {
                    tracing::debug!(session = %id, "connection cancelled, disconnecting");
                    return Ok(());
                }
                proto::ProtoOp::Echo { msg } => {
                    proto.write_echo(&mut writer, &msg).await?;
                    proto.flush(&mut writer).await?;
                }
                proto::ProtoOp::Get { key } => {
                    let val = self.store.get(&key).await.unwrap();
                    if let Some(val) = val {
                        proto.write_get_result(&mut writer, &val).await?;
                        proto.flush(&mut writer).await?;
                    } else {
                        proto.write_null(&mut writer).await?;
                        proto.flush(&mut writer).await?;
                    }
                }
                proto::ProtoOp::Set { key, value } => {
                    self.store
                        .transact(Transaction::with_random_id(vec![Operation::set(
                            key,
                            value.as_slice(),
                        )]))
                        .await
                        .ok();
                    proto.write_set_result(&mut writer, &value).await?;
                    proto.flush(&mut writer).await?;
                }
            }
        }
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
        kill: Receiver<bool>,
    ) -> Result<()> {
        uuid_with_ident!(id);
        tracing::info!(session = id, "client connected");
        let (stream, peer_addr) =
            stream_peer_addr_res.map_err(|e| format!("session={id} error accepting tls: {e}"))?;
        let conn = Connection::new(id.to_string(), stream, peer_addr, acceptor, store, kill);
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
        let (kill_send, _) = broadcast::channel(1);

        loop {
            tokio::select! {
                _ = self.sig_shutdown_recv.recv() => {
                    tracing::info!("client-server received sigint shutdown signal");
                    kill_send.send(true).expect("error broadcasting task kill");
                    break;
                },
                stream_peer_addr_res = listener.accept() => {
                    let acceptor = acceptor.clone();
                    let store = self.store.clone();
                    let kill = kill_send.subscribe();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_conn(stream_peer_addr_res, acceptor, store, kill).await {
                            tracing::error!("error handling client connection {e}");
                        }
                    });
                },
                // _ = tokio::time::sleep(tokio::time::Duration::from_millis(500)) => {
                //     tracing::trace!("client-server slept 500ms...");
                // },
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
