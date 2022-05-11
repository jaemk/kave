#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    E(String),

    #[error("io error: {0}")]
    IO(#[from] std::io::Error),

    #[error("parseint error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("bincode error: {0}")]
    BincodeError(#[from] bincode::Error),

    #[error("timeout error: {0}")]
    TimeoutError(#[from] tokio::time::error::Elapsed),

    #[error("dns resolve error: {0}")]
    DnsResolveError(#[from] trust_dns_resolver::error::ResolveError),

    #[error("failure resolving dns for: {0}")]
    DnsResolutionFailure(String),
}
impl From<&str> for Error {
    fn from(s: &str) -> Error {
        Error::E(s.to_string())
    }
}
impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::E(s)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
