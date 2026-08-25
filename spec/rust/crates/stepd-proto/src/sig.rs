//! Request signing.
//!
//! `stepd-signature: t=<unix>,n=<nonce>,v1=<hex hmac>` where the MAC covers
//! `"<t>.<nonce>.<body>"`.
//!
//! The nonce is not decoration. A timestamp window alone permits replay of a
//! captured body for the width of that window, so receivers must also keep a
//! seen-nonce cache. Including the nonce in the MAC prevents an attacker from
//! swapping in a fresh one.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Default acceptance window either side of the signature timestamp.
pub const DEFAULT_TOLERANCE_SECS: i64 = 300;

/// Why a signature was rejected.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SignatureError {
    /// Header absent or not in the documented form.
    #[error("malformed signature header")]
    Malformed,
    /// Timestamp outside the tolerance window.
    #[error("signature timestamp outside tolerance window")]
    Expired,
    /// MAC did not match.
    #[error("signature mismatch")]
    Mismatch,
    /// Nonce already seen inside the window.
    #[error("nonce replayed")]
    Replay,
}

/// Produce a signature header value.
pub fn sign(key: &[u8], body: &str, unix_ts: i64, nonce: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(format!("{unix_ts}.{nonce}.{body}").as_bytes());
    format!(
        "t={unix_ts},n={nonce},v1={}",
        hex::encode(mac.finalize().into_bytes())
    )
}

/// Verify a signature header against one or more accepted keys.
///
/// Multiple keys are accepted so that key rotation does not require a flag day:
/// during rotation both the current and previous key are live.
pub fn verify(
    keys: &[Vec<u8>],
    header: &str,
    body: &str,
    now_unix: i64,
    tolerance_secs: i64,
) -> Result<String, SignatureError> {
    let (mut ts, mut nonce, mut mac_hex) = (None, None, None);
    for part in header.split(',') {
        match part.split_once('=') {
            Some(("t", v)) => ts = v.trim().parse::<i64>().ok(),
            Some(("n", v)) => nonce = Some(v.trim().to_string()),
            Some(("v1", v)) => mac_hex = Some(v.trim().to_string()),
            _ => {}
        }
    }
    let (ts, nonce, mac_hex) = match (ts, nonce, mac_hex) {
        (Some(a), Some(b), Some(c)) => (a, b, c),
        _ => return Err(SignatureError::Malformed),
    };
    if (now_unix - ts).abs() > tolerance_secs {
        return Err(SignatureError::Expired);
    }
    let provided = hex::decode(&mac_hex).map_err(|_| SignatureError::Malformed)?;
    for key in keys {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(format!("{ts}.{nonce}.{body}").as_bytes());
        let expected = mac.finalize().into_bytes();
        // Constant-time: a byte-by-byte comparison leaks the correct prefix via timing.
        if expected.ct_eq(&provided).into() {
            return Ok(nonce);
        }
    }
    Err(SignatureError::Mismatch)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"secret-key";
    const BODY: &str = r#"{"run_id":"abc"}"#;

    fn keys() -> Vec<Vec<u8>> {
        vec![KEY.to_vec()]
    }

    #[test]
    fn round_trip() {
        let h = sign(KEY, BODY, 1000, "n1");
        assert_eq!(verify(&keys(), &h, BODY, 1000, 300).unwrap(), "n1");
    }

    #[test]
    fn rejects_tampered_body() {
        let h = sign(KEY, BODY, 1000, "n1");
        assert_eq!(
            verify(&keys(), &h, r#"{"run_id":"evil"}"#, 1000, 300),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn rejects_wrong_key() {
        let h = sign(b"other", BODY, 1000, "n1");
        assert_eq!(
            verify(&keys(), &h, BODY, 1000, 300),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn rejects_expired_and_future_timestamps() {
        let h = sign(KEY, BODY, 1000, "n1");
        assert_eq!(
            verify(&keys(), &h, BODY, 1400, 300),
            Err(SignatureError::Expired)
        );
        assert_eq!(
            verify(&keys(), &h, BODY, 600, 300),
            Err(SignatureError::Expired)
        );
    }

    #[test]
    fn nonce_is_covered_by_the_mac() {
        // Swapping the nonce must invalidate the MAC, or replay defence is bypassable.
        let h = sign(KEY, BODY, 1000, "n1").replace("n=n1", "n=n2");
        assert_eq!(
            verify(&keys(), &h, BODY, 1000, 300),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn rotation_accepts_previous_key() {
        let both = vec![b"new-key".to_vec(), KEY.to_vec()];
        let h = sign(KEY, BODY, 1000, "n1");
        assert!(verify(&both, &h, BODY, 1000, 300).is_ok());
    }

    #[test]
    fn malformed_headers_rejected() {
        for h in ["", "garbage", "t=1000", "t=abc,n=x,v1=00", "n=x,v1=00"] {
            assert!(
                matches!(
                    verify(&keys(), h, BODY, 1000, 300),
                    Err(SignatureError::Malformed) | Err(SignatureError::Mismatch)
                ),
                "accepted: {h}"
            );
        }
    }
}
