//! Serves the GUI baked into the binary by `build.rs`.

use axum::body::Body;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

include!(concat!(env!("OUT_DIR"), "/embedded_assets.rs"));

/// One bundler's output needs one short table. Unknown extensions download rather than being guessed at.
fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("map") => "application/json",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        Some("ico") => "image/vnd.microsoft.icon",
        _ => "application/octet-stream",
    }
}

/// A scan, so the lookup holds whatever order `build.rs` writes the table in. A Vite build is a few dozen files.
fn lookup(path: &str) -> Option<&'static [u8]> {
    ASSETS.iter().find(|(name, _)| *name == path).map(|(_, bytes)| *bytes)
}

/// Whether this build carries only `build.rs`'s placeholder page. A real Vite build always emits hashed files under
/// `assets/`.
pub fn is_placeholder_only() -> bool {
    ASSETS.len() == 1 && ASSETS[0].0 == "index.html"
}

/// Serves an embedded asset. A path that is plainly not a file falls back
/// to `index.html`, so a client-side route survives a refresh; a missing
/// asset gets a 404 rather than HTML.
pub async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    if let Some(bytes) = lookup(path) {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type(path))],
            Body::from(bytes),
        )
            .into_response();
    }

    let looks_like_a_file = path.starts_with("assets/") || path.rsplit('/').next().is_some_and(|f| f.contains('.'));
    if looks_like_a_file {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }

    match lookup("index.html") {
        Some(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type("index.html"))],
            Body::from(bytes),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_is_always_an_index() {
        assert!(lookup("index.html").is_some());
    }

    #[test]
    fn content_types_cover_what_the_bundler_emits() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("assets/index-BoXouX0e.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("assets/index-WMnJLfry.css"), "text/css; charset=utf-8");
        assert_eq!(content_type("favicon.svg"), "image/svg+xml");
        assert_eq!(content_type("weird.xyz"), "application/octet-stream");
        assert_eq!(content_type("noextension"), "application/octet-stream");
    }
}
