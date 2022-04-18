use crate::error::Error;
fn env_or(k: &str, default: &str) -> String {
    tracing::debug!("loading env var: {k:?}");
    std::env::var(k).unwrap_or_else(|_| default.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}
impl std::str::FromStr for LogFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<LogFormat, Error> {
        match s.trim().to_lowercase().as_str() {
            "" | "json" => Ok(LogFormat::Json),
            "pretty" => Ok(LogFormat::Pretty),
            s => Err(Error::from(format!(
                "invalid LOG_FORMAT: {s}, expected one of (pretty|json)"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    // host to listen on, defaults to localhost
    pub host: String,
    pub client_port: u16,
    pub cluster_port: u16,

    pub seed_peers: Vec<String>,
    pub cert_path: String,
    pub key_path: String,

    pub log_level: String,
    pub log_format: LogFormat,

    // key used for encrypting things
    pub encryption_key: String,

    // key used for signing/hashing things
    pub signing_key: String,
}
impl Config {
    pub fn load() -> Self {
        Self {
            host: env_or("HOST", "localhost"),
            client_port: env_or("CLIENT_PORT", "7719").parse().expect("invalid port"),
            cluster_port: env_or("CLUSTER_PORT", "7720")
                .parse()
                .expect("invalid port"),
            seed_peers: env_or("SEED_PEERS", "")
                .trim()
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>(),
            cert_path: env_or("CERT_PATH", "certs/cert.pem"),
            key_path: env_or("KEY_PATH", "certs/key.pem"),
            log_level: env_or("LOG_LEVEL", "info"),
            log_format: env_or("LOG_FORMAT", "pretty")
                .parse()
                .expect("invalid LOG_FORMAT"),
            encryption_key: env_or("ENCRYPTION_KEY", "01234567890123456789012345678901"),
            signing_key: env_or("SIGNING_KEY", "01234567890123456789012345678901"),
        }
    }
    pub fn get_cluster_addr(&self) -> String {
        format!("{}:{}", self.host, self.cluster_port)
    }
    pub fn get_client_addr(&self) -> String {
        format!("{}:{}", self.host, self.client_port)
    }
}
