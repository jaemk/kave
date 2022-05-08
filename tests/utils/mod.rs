use kave::server::load_certs;
use kave::{client, Result};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// create a new tls stream to a given address
pub async fn connect(addr: &str) -> Result<TlsStream<TcpStream>> {
    let certs = load_certs("certs/defaults/cert.pem").expect("error loading default test certs");
    client::connect(addr, certs).await
}

/// init logger and other stuff
#[macro_export]
macro_rules! init {
    () => {{
        init!(std::env::var("LOG_LEVEL").unwrap_or_else(|_| "error".to_string()));
    }};
    ($log_level:expr) => {{
        let filter = tracing_subscriber::filter::EnvFilter::new($log_level);
        let sub = tracing_subscriber::fmt().with_env_filter(filter);
        sub.try_init().ok();
    }};
}

/// read from $reader into a buf until at least $min_bytes exist
#[macro_export]
macro_rules! read_buf {
    ($reader:expr, $min_bytes:expr) => {{
        use tokio::io::AsyncReadExt;
        let mut buf = vec![];
        while buf.len() < $min_bytes {
            $reader.read_buf(&mut buf).await.expect("error reading");
        }
        buf
    }};
}
