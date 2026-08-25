//! Serving the operations console.
//!
//! The console is one self-contained HTML file compiled into the binary — no
//! asset directory to deploy, no CDN, no version skew between the API and the UI
//! that talks to it. `stepd serve` is the whole product.
//!
//! ## Content-Security-Policy
//!
//! F-SEC-4 requires a strict CSP, and the console renders arbitrary payload JSON
//! and app-supplied `$ref` URIs — so a stored-XSS path exists by construction if
//! escaping ever slips. The console escapes by default; the CSP is the second
//! layer, for the day the first one has a hole.
//!
//! `'unsafe-inline'` is not used. The console's single inline `<script>` gets a
//! fresh nonce per response and the policy admits exactly that nonce, which is a
//! strict CSP that still allows a single-file console. Blanket `'unsafe-inline'`
//! would admit *any* injected script tag as well, which is the whole attack.

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

/// The console, compiled in.
const CONSOLE_HTML: &str = include_str!("../assets/console.html");

/// Build the response for `GET /`.
pub async fn serve() -> Response {
    // 128 bits, base16. A predictable nonce is no nonce: an attacker who can
    // guess it can mark their injected script as trusted.
    let nonce = {
        use rand::RngCore;
        let mut b = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut b);
        hex::encode(b)
    };

    let html = CONSOLE_HTML.replacen("<script>", &format!("<script nonce=\"{nonce}\">"), 1);

    let csp = format!(
        "default-src 'none'; \
         script-src 'nonce-{nonce}'; \
         style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; \
         font-src https://fonts.gstatic.com; \
         connect-src 'self'; \
         img-src 'self' data:; \
         base-uri 'none'; \
         form-action 'none'; \
         frame-ancestors 'none'"
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        "content-security-policy",
        HeaderValue::from_str(&csp).unwrap(),
    );
    // Defence in depth for browsers and for anything that proxies this response.
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    // The console takes its token from the query string, so the URL is a
    // credential. Keep it out of Referer headers and out of shared caches.
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );

    (StatusCode::OK, headers, html).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_console_is_compiled_into_the_binary() {
        assert!(CONSOLE_HTML.contains("<title>stepd console</title>"));
        assert!(
            CONSOLE_HTML.len() > 5_000,
            "an empty or truncated asset would serve a blank page with a 200"
        );
    }

    #[test]
    fn the_console_escapes_by_default() {
        // The CSP is the second layer. This asserts the first one is still there:
        // the console renders arbitrary payload JSON, so an escaping helper that
        // covers every dangerous character is not optional (F-SEC-4).
        assert!(CONSOLE_HTML.contains("const esc"));
        for entity in ["&amp;", "&lt;", "&gt;", "&quot;", "&#39;"] {
            assert!(
                CONSOLE_HTML.contains(entity),
                "escape helper is missing {entity}"
            );
        }
    }

    #[tokio::test]
    async fn every_response_carries_a_fresh_nonce_and_no_unsafe_inline() {
        let a = serve().await;
        let csp_a = a
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        assert!(
            !csp_a.contains("'unsafe-inline'") || !csp_a.contains("script-src 'unsafe-inline'"),
            "script-src must never admit unsafe-inline: it would admit an injected tag too"
        );
        assert!(csp_a.contains("script-src 'nonce-"));
        assert!(csp_a.contains("default-src 'none'"));
        assert!(csp_a.contains("frame-ancestors 'none'"), "clickjacking");
        assert!(csp_a.contains("base-uri 'none'"), "base-tag injection");

        let b = serve().await;
        let csp_b = b
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert_ne!(csp_a, csp_b, "a reused nonce is not a nonce");
    }

    #[tokio::test]
    async fn the_console_page_is_never_cached() {
        // The token is in the query string, so the URL is a credential.
        let res = serve().await;
        let cc = res
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cc.contains("no-store"));
        assert_eq!(res.headers().get("referrer-policy").unwrap(), "no-referrer");
    }
}
