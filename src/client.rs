use std::net::IpAddr;

use crate::error::{Error, Result};
use tokio::net::TcpStream;
use tokio_rustls::{
    client::TlsStream,
    rustls::{
        self,
        client::{ServerCertVerified, ServerCertVerifier, ServerName},
        Certificate,
    },
    TlsConnector,
};
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    TokioAsyncResolver,
};

/// A client verifier to only check if the server cert
/// exactly matches what the client is configured to use.
/// The default webpki ServerCertVerifier will compare
/// against trusted root certs and verify the domain name.
pub struct ExactCertVerifier {
    certs: Vec<Certificate>,
}
impl ExactCertVerifier {
    pub fn new(certs: Vec<Certificate>) -> Self {
        Self { certs }
    }
}

impl ServerCertVerifier for ExactCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        if self.certs.iter().any(|cert| end_entity == cert) {
            Ok(rustls::client::ServerCertVerified::assertion())
        } else {
            tracing::error!("server end end_entity does not match any client cert");
            tracing::debug!("end_entity: {:?}", end_entity);
            tracing::debug!("client certs: {:?}", self.certs);
            Err(rustls::Error::InvalidCertificateData(
                "server certificate does not match what the client expected".into(),
            ))
        }
    }
}

/// Return the first IpAddr found for a hostname
pub async fn resolve_dns(host: &str) -> Result<Option<IpAddr>> {
    // TODO: support more configuration so you can specify a
    //       custom dns server
    tracing::debug!("resolving dns for {host}");
    let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
        .map_err(|e| format!("error constructing DNS resolver client: {e}"))?;
    let ip = resolver.lookup_ip(host).await?.iter().next();
    Ok(ip)
}

/// Create a new tls stream to a given address using
/// an exact-cert verifier instead of a typical root cert verifier.
/// This can only connect to servers exposing a certificate listed in `certs`
pub async fn connect(
    addr: &str,
    port: u16,
    certs: Vec<Certificate>,
) -> Result<TlsStream<TcpStream>> {
    let addr = match resolve_dns(addr).await? {
        None => return Err(Error::DnsResolutionFailure(addr.into())),
        Some(ip_addr) => ip_addr,
    };
    let mut config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    // configure the client to only check for an exact cert match instead
    // of checking the domain name and comparing against a cert chain.
    config
        .dangerous()
        .set_certificate_verifier(std::sync::Arc::new(ExactCertVerifier::new(certs)));

    let connector = TlsConnector::from(std::sync::Arc::new(config));
    tracing::debug!("connecting to {addr}:{port}");
    let stream = TcpStream::connect((addr, port)).await?;

    // need to pass something that's a valid domain name, but it doesn't matter what it is
    // because our client will only check for an exact certificate match
    let domain =
        ServerName::try_from("bread.com").map_err(|e| format!("error parsing host: {e}"))?;
    let stream = connector.connect(domain, stream).await?;
    Ok(stream)
}
