use kave::client;
use kave::server::{load_certs, load_keys, ClientServer};
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_rustls::client::TlsStream;

fn new_client_server() -> (UnboundedSender<bool>, UnboundedReceiver<bool>, ClientServer) {
    let certs = load_certs("certs/defaults/cert.pem").expect("error loading default test certs");
    let keys = load_keys("certs/defaults/key.pem").expect("error loading default test keys");
    let (client_svr_shutdown_send, client_svr_shutdown_recv) =
        tokio::sync::mpsc::unbounded_channel();
    let (sig_client_shutdown_send, sig_client_shutdown_recv) =
        tokio::sync::mpsc::unbounded_channel();

    let client_svr = ClientServer::new(
        client_svr_shutdown_send,
        sig_client_shutdown_recv,
        certs,
        keys,
    );
    (
        sig_client_shutdown_send,
        client_svr_shutdown_recv,
        client_svr,
    )
}

/// create a new tls stream to a given address
async fn connect(addr: &str) -> TlsStream<TcpStream> {
    let certs = load_certs("certs/defaults/cert.pem").expect("error loading default test certs");
    client::connect(addr, certs)
        .await
        .expect("error connecting to test addr")
}

/// init logger and other stuff
macro_rules! init {
    () => {{
        init!(std::env::var("LOG_LEVEL").unwrap_or_else(|_| "error".to_string()));
    }};
    ($log_level:expr) => {{
        let filter = tracing_subscriber::filter::EnvFilter::new($log_level);
        let sub = tracing_subscriber::fmt().with_env_filter(filter);
        sub.init();
    }};
}

/// create a new client server and wait for it start
macro_rules! start_client_server {
    ($port:expr) => {{
        let (shutdown_send, shutdown_recv, mut cs) = new_client_server();
        cs.set_port($port);
        tokio::spawn(async move { cs.start().await });
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        (shutdown_send, shutdown_recv)
    }};
}

#[tokio::test]
async fn test_client_server_basic() {
    init!();
    let (shutdown_send, mut shutdown_recv) = start_client_server!(7333);

    let stream = connect("localhost:7333").await;
    let (mut reader, mut writer) = split(stream);

    writer
        .write_all(b"working!!!")
        .await
        .expect("error writing");

    // give it a sec to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut buf = vec![];
    reader.read_buf(&mut buf).await.expect("error reading");
    assert_eq!(buf, b"working!!!");

    // send shutdown and assert that it actually shuts down
    shutdown_send
        .send(true)
        .expect("error sending client-server shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown_recv.recv())
        .await
        .expect("client-server failed to shutdown");
}
