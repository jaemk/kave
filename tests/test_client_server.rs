use kave::server::{load_certs, load_keys, ClientServer};
use kave::store::MemoryStore;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[macro_use]
mod utils;

fn new_client_server() -> (
    UnboundedSender<bool>,
    UnboundedReceiver<bool>,
    ClientServer<MemoryStore>,
) {
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
        MemoryStore::new(),
    );
    (
        sig_client_shutdown_send,
        client_svr_shutdown_recv,
        client_svr,
    )
}

/// create a new client server and wait for it start
macro_rules! start_client_server {
    ($addr:expr) => {{
        let (shutdown_send, shutdown_recv, mut cs) = new_client_server();
        cs.set_addr($addr);
        tokio::spawn(async move { cs.start().await });
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        (shutdown_send, shutdown_recv)
    }};
}

#[tokio::test]
async fn test_client_server_basic() {
    init!();
    let (shutdown_send, mut shutdown_recv) = start_client_server!("localhost:7310");

    let stream = utils::connect("localhost:7310")
        .await
        .expect("error connecting to test addr");
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

#[tokio::test]
async fn test_client_server_get_set() {
    init!();
    let (shutdown_send, mut shutdown_recv) = start_client_server!("localhost:7311");

    let stream = utils::connect("localhost:7311")
        .await
        .expect("error connecting to test addr");
    let (mut reader, mut writer) = split(stream);

    // get non existing key
    writer
        .write_all(b"GET:5:abcdef")
        .await
        .expect("error writing");
    let mut buf = vec![];
    reader.read_buf(&mut buf).await.expect("error reading");
    assert_eq!(buf, b"4:null\n");

    // set missing key
    writer
        .write_all(b"SET:5:abcde:30:012345678901234567890123456789-this-should-be-ignored")
        .await
        .expect("error writing");
    let mut buf = vec![];
    reader.read_buf(&mut buf).await.expect("error reading");
    assert_eq!(buf, b"2:ok:2:30\n");

    // get previously set key
    writer
        .write_all(b"GET:5:abcde-this-should-be-ignored")
        .await
        .expect("error writing");
    let mut buf = vec![];
    reader.read_buf(&mut buf).await.expect("error reading");
    // assert_eq!(std::str::from_utf8(&buf).unwrap(), "");
    assert_eq!(buf, b"30:012345678901234567890123456789\n");

    // send shutdown and assert that it actually shuts down
    shutdown_send
        .send(true)
        .expect("error sending client-server shutdown");
    tokio::time::timeout(std::time::Duration::from_secs(5), shutdown_recv.recv())
        .await
        .expect("client-server failed to shutdown");
}
