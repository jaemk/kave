use crate::error::Error;
fn env_or(k: &str, default: &str) -> String {
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

pub struct Config {
    // host to listen on, defaults to localhost
    pub host: String,
    pub port: u16,

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
            port: env_or("PORT", "3003").parse().expect("invalid port"),
            log_level: env_or("LOG_LEVEL", "info"),
            log_format: env_or("LOG_FORMAT", "pretty")
                .parse()
                .expect("invalid LOG_FORMAT"),
            encryption_key: env_or("ENCRYPTION_KEY", "01234567890123456789012345678901"),
            signing_key: env_or("SIGNING_KEY", "01234567890123456789012345678901"),
        }
    }
    pub fn initialize(&self) {
        use crate::CONFIG;
        tracing::info!(
            host = %CONFIG.host,
            port = %CONFIG.port,
            log_level = %CONFIG.log_level,
            "initialized config",
        );
    }
    pub fn get_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
