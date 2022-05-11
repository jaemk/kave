use crate::error::Result;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use tokio_rustls::rustls::{Certificate, PrivateKey};

mod client;
mod cluster;

pub use client::ClientServer;
pub use cluster::Server;

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
