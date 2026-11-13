//! rustls configuration: TLS 1.3 only, the ring provider, no resumption.
use super::{Tls, invalid};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    static PROVIDER: OnceLock<Arc<rustls::crypto::CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
        .clone()
}

pub(super) fn error(what: &str, e: rustls::Error) -> io::Error {
    let kind = match &e {
        rustls::Error::InvalidCertificate(_)
        | rustls::Error::NoCertificatesPresented
        | rustls::Error::AlertReceived(rustls::AlertDescription::CertificateRequired)
        | rustls::Error::AlertReceived(rustls::AlertDescription::BadCertificate)
        | rustls::Error::AlertReceived(rustls::AlertDescription::UnknownCA) => {
            io::ErrorKind::PermissionDenied
        }
        _ => io::ErrorKind::InvalidData,
    };
    let hint = match &e {
        rustls::Error::AlertReceived(rustls::AlertDescription::CertificateRequired) => {
            " (the listener requires a client certificate: add cert= and key=)"
        }
        _ => "",
    };
    io::Error::new(kind, format!("{what}: {e}{hint}"))
}

pub(super) fn load(
    cert: Option<(&Path, &[u8])>,
    key: Option<(&Path, &[u8])>,
    ca: Option<(&Path, &[u8])>,
) -> io::Result<Tls> {
    let bad = |path: &Path, what: &str, e: &dyn std::fmt::Display| {
        invalid(format!("{}: {what}: {e}", path.display()))
    };
    let certs = match cert {
        None => Vec::new(),
        Some((path, pem)) => {
            let certs = CertificateDer::pem_slice_iter(pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| bad(path, "invalid certificate PEM", &e))?;
            if certs.is_empty() {
                return Err(invalid(format!(
                    "{}: no certificate in the file",
                    path.display()
                )));
            }
            certs
        }
    };
    let key = key
        .map(|(path, pem)| {
            PrivateKeyDer::from_pem_slice(pem)
                .map(Arc::new)
                .map_err(|e| bad(path, "no usable private key", &e))
        })
        .transpose()?;
    if certs.is_empty() != key.is_none() {
        return Err(invalid(
            "a TLS certificate and its private key go together: give both",
        ));
    }
    let roots = ca
        .map(|(path, pem)| {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| bad(path, "invalid CA PEM", &e))?;
                roots
                    .add(cert)
                    .map_err(|e| bad(path, "unusable CA certificate", &e))?;
            }
            if roots.is_empty() {
                return Err(invalid(format!(
                    "{}: no CA certificate in the file",
                    path.display()
                )));
            }
            Ok(Arc::new(roots))
        })
        .transpose()?;
    if let Some(key) = &key {
        // Fail at startup, not at the first handshake.
        provider()
            .key_provider
            .load_private_key(key.clone_key())
            .map_err(|e| invalid(format!("the private key cannot sign: {e}")))?;
    }
    Ok(Tls {
        certs,
        key,
        roots,
        server_name: None,
    })
}

pub(super) fn server_config(tls: &Tls) -> io::Result<Arc<rustls::ServerConfig>> {
    let Some(key) = &tls.key else {
        return Err(invalid(
            "a TLS listener needs its certificate and private key",
        ));
    };
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| error("TLS configuration", e))?;
    let builder = match &tls.roots {
        Some(roots) => {
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                roots.clone(),
                provider(),
            )
            .build()
            .map_err(|e| invalid(format!("client certificate verifier: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(tls.certs.clone(), key.clone_key())
        .map_err(|e| error("TLS certificate", e))?;
    // Connections live long and reconnect rarely: no tickets, no resumption.
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    Ok(Arc::new(config))
}

pub(super) fn client_config(tls: &Tls) -> io::Result<Arc<rustls::ClientConfig>> {
    let Some(roots) = &tls.roots else {
        return Err(invalid(
            "TLS needs a CA certificate to verify the server (ca=PATH)",
        ));
    };
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| error("TLS configuration", e))?
        .with_root_certificates(roots.clone());
    let mut config = match &tls.key {
        Some(key) => builder
            .with_client_auth_cert(tls.certs.clone(), key.clone_key())
            .map_err(|e| error("TLS client certificate", e))?,
        None => builder.with_no_client_auth(),
    };
    config.resumption = rustls::client::Resumption::disabled();
    Ok(Arc::new(config))
}
