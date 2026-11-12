//! The web UI's files, built from `ui/` (React) into `ui-dist/` and
//! checked in, so building the crate never needs Node. Embedded with the
//! `ui` feature (on by default); without it the page says so.

use super::{HttpOptions, Response};

#[cfg(feature = "ui")]
mod files {
    include!("ui-dist/files.rs");
}

#[cfg(feature = "ui")]
pub(super) fn page(opts: &HttpOptions) -> Response {
    // The page may call the operator's codec endpoint, and nothing else.
    let codec = opts
        .codec_url
        .as_deref()
        .and_then(super::origin_of)
        .map(|o| format!(" {o}"))
        .unwrap_or_default();
    Response {
        status: 200,
        content_type: "text/html; charset=utf-8",
        body: files::INDEX.to_vec(),
        extra: vec![
            (
                "Content-Security-Policy",
                format!(
                    "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; font-src 'self'; \
                     connect-src 'self'{codec}; frame-ancestors 'none'"
                ),
            ),
            ("Referrer-Policy", "no-referrer".into()),
        ],
    }
}

#[cfg(feature = "ui")]
pub(super) fn asset(name: &str) -> Response {
    match files::ASSETS.iter().find(|(n, _, _)| *n == name) {
        Some((_, ty, body)) => Response {
            status: 200,
            content_type: ty,
            body: body.to_vec(),
            extra: vec![(
                "Cache-Control",
                "public, max-age=31536000, immutable".into(),
            )],
        },
        None => Response::error(404, "NotFound", "no such asset"),
    }
}

#[cfg(not(feature = "ui"))]
pub(super) fn page(_opts: &HttpOptions) -> Response {
    Response {
        status: 200,
        content_type: "text/html; charset=utf-8",
        body: b"<!doctype html><title>Fusee</title><p>This build of Fusee has no web UI (the <code>ui</code> \
            feature is off). The API is at <a href=\"/v1/openapi.json\">/v1</a>."
            .to_vec(),
        extra: Vec::new(),
    }
}

#[cfg(not(feature = "ui"))]
pub(super) fn asset(_name: &str) -> Response {
    Response::error(404, "NotFound", "this build has no web UI")
}
