//! # Signed HTTP push transport
//!
//! The server calls the app; the app answers with an op envelope. Both
//! directions are signed (protocol §9), and every URL the server dereferences
//! passes an egress policy first.
//!
//! ## Why the egress policy is not optional
//!
//! `app.url` comes from a registration payload — that is, from a tenant. A
//! server that fetches it without restriction will happily fetch
//! `http://169.254.169.254/latest/meta-data/iam/security-credentials/`, and hand
//! the response to whoever registered the app. That is not a theoretical attack;
//! it is the standard way a multi-tenant orchestrator leaks its own cloud
//! credentials.
//!
//! [`EgressPolicy`] therefore denies link-local, cloud-metadata, loopback and
//! private ranges unless explicitly allowlisted, resolves DNS once and connects
//! to *that* address (so a name cannot resolve differently between the check and
//! the connection), refuses redirects outright, and caps the response size.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use stepd_core::traits::{AppTarget, Transport};
use stepd_core::Error;
use stepd_proto::{sig, Attempt, AttemptResponse, PROTOCOL_VERSION};
use tracing::warn;

/// What the server is allowed to connect to.
#[derive(Debug, Clone)]
pub struct EgressPolicy {
    /// Permit loopback. Development only.
    pub allow_loopback: bool,
    /// Permit RFC 1918 and equivalent ranges. Required for a server and its apps
    /// inside one private network, which is the common single-tenant deployment.
    pub allow_private: bool,
    /// Hosts permitted regardless of the range rules.
    pub allowlist: Vec<String>,
    /// Ceiling on a response body.
    pub max_body_bytes: usize,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        // Fails closed. A deployment that needs private addressing says so; one
        // that forgets gets an error at registration, not a credential leak.
        Self {
            allow_loopback: false,
            allow_private: false,
            allowlist: Vec::new(),
            max_body_bytes: 8 * 1024 * 1024,
        }
    }
}

impl EgressPolicy {
    /// The policy `stepd dev` uses: loopback and private ranges permitted.
    pub fn development() -> Self {
        Self {
            allow_loopback: true,
            allow_private: true,
            ..Default::default()
        }
    }

    /// Whether an address may be connected to.
    pub fn allows(&self, ip: IpAddr) -> Result<(), String> {
        // Cloud metadata is denied unconditionally — not even `allow_private`
        // opens it, because no legitimate app endpoint lives there and the cost
        // of being wrong is every credential the server holds.
        if is_metadata(ip) {
            return Err(format!("{ip} is a cloud metadata address"));
        }
        if ip.is_loopback() && !self.allow_loopback {
            return Err(format!("{ip} is loopback and loopback is not allowed"));
        }
        if is_link_local(ip) {
            return Err(format!("{ip} is link-local"));
        }
        if is_private(ip) && !self.allow_private {
            return Err(format!(
                "{ip} is in a private range and private ranges are not allowed"
            ));
        }
        if is_unspecified_or_broadcast(ip) {
            return Err(format!("{ip} is not a routable destination"));
        }
        Ok(())
    }

    /// Resolve a URL and return the single address to connect to.
    ///
    /// One resolution, one address, and the connection is pinned to it. Checking
    /// a hostname and then letting the HTTP client resolve it again is a
    /// DNS-rebinding hole: the second lookup can return the metadata address
    /// after the first returned something innocuous.
    pub fn resolve(&self, url: &str) -> Result<(String, IpAddr, u16), String> {
        let parsed = url::Url::parse(url).map_err(|e| format!("invalid url: {e}"))?;
        match parsed.scheme() {
            "http" | "https" => {}
            other => return Err(format!("scheme '{other}' is not permitted")),
        }
        let host = parsed.host_str().ok_or("url has no host")?.to_string();
        let port = parsed
            .port_or_known_default()
            .ok_or("url has no port and no default for its scheme")?;

        if self.allowlist.iter().any(|h| h == &host) {
            // An allowlisted host still needs an address, but skips the range
            // checks: an operator naming a host has made the decision explicitly.
            let ip = first_address(&host, port)?;
            return Ok((host, ip, port));
        }

        let ip = first_address(&host, port)?;
        self.allows(ip)?;
        Ok((host, ip, port))
    }
}

fn first_address(host: &str, port: u16) -> Result<IpAddr, String> {
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{host}': {e}"))?
        .next()
        .map(|a| a.ip())
        .ok_or_else(|| format!("'{host}' resolved to no addresses"))
}

fn is_metadata(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.octets() == [169, 254, 169, 254],
        // fd00:ec2::254 — the AWS IPv6 metadata endpoint, and the fd00:ec2::/32
        // block it lives in.
        IpAddr::V6(v6) => {
            let s = v6.segments();
            s[0] == 0xfd00 && s[1] == 0x0ec2
        }
    }
}

fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xffc0) == 0xfe80,
    }
}

fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private() || v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1])
            // CGNAT
        }
        // Unique local addresses, fc00::/7.
        IpAddr::V6(v6) => (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

fn is_unspecified_or_broadcast(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_unspecified() || v4.is_broadcast(),
        IpAddr::V6(v6) => v6.is_unspecified(),
    }
}

/// Push transport over HTTP.
pub struct HttpTransport {
    /// Clients pinned to one resolved address each.
    ///
    /// This is what closes the rebinding hole. Checking a hostname's address and
    /// then handing the *hostname* to an HTTP client lets the client resolve it
    /// a second time, and the second answer can be the metadata address after
    /// the first was innocuous. Pinning means the connection goes to the address
    /// the policy actually approved.
    pinned: std::sync::Mutex<HashMap<(String, u16, IpAddr), reqwest::Client>>,
    timeout: Duration,
    policy: EgressPolicy,
}

impl HttpTransport {
    /// Build a transport with an attempt timeout and an egress policy.
    pub fn new(timeout: Duration, policy: EgressPolicy) -> Result<Self, Error> {
        // Probe the builder once so a bad TLS configuration fails at start-up
        // rather than on the first dispatch.
        Self::client_for(timeout, "probe.invalid", 443, IpAddr::from([127, 0, 0, 1]))?;
        Ok(Self {
            pinned: std::sync::Mutex::new(HashMap::new()),
            timeout,
            policy,
        })
    }

    fn client_for(
        timeout: Duration,
        host: &str,
        port: u16,
        ip: IpAddr,
    ) -> Result<reqwest::Client, Error> {
        reqwest::Client::builder()
            .timeout(timeout)
            // Zero redirects. A redirect is a second URL the tenant chose, and
            // following one would let an allowlisted host bounce the server
            // anywhere at all — the policy would have checked the wrong address.
            .redirect(reqwest::redirect::Policy::none())
            // Resolve once, connect to that. The address is the one the policy
            // approved, not whatever DNS says a moment later.
            .resolve(host, std::net::SocketAddr::new(ip, port))
            .build()
            .map_err(|e| Error::Config(e.to_string()))
    }

    /// A client whose DNS for `url`'s host is pinned to the approved address.
    fn approved_client(&self, url: &str) -> Result<reqwest::Client, Error> {
        let (host, ip, port) = self
            .policy
            .resolve(url)
            .map_err(|e| Error::Config(format!("egress policy refused {url}: {e}")))?;

        let key = (host.clone(), port, ip);
        if let Some(c) = self.pinned.lock().unwrap().get(&key) {
            return Ok(c.clone());
        }
        let client = Self::client_for(self.timeout, &host, port, ip)?;
        // Bounded: an unbounded map keyed by tenant-supplied hostnames is a
        // memory-growth surface. Apps are few and long-lived, so clearing on
        // overflow costs one extra connection setup and nothing else.
        let mut pinned = self.pinned.lock().unwrap();
        if pinned.len() > 1024 {
            pinned.clear();
        }
        pinned.insert(key, client.clone());
        Ok(client)
    }

    /// The egress policy in force.
    pub fn policy(&self) -> &EgressPolicy {
        &self.policy
    }

    /// Fetch an app's discovery manifest (protocol §3, pull mode).
    pub async fn fetch_manifest(&self, base_url: &str) -> Result<serde_json::Value, Error> {
        let url = format!("{}/.well-known/stepd", base_url.trim_end_matches('/'));
        let client = self.approved_client(&url)?;
        let res = client
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let body = self.read_bounded(res).await?;
        serde_json::from_str(&body).map_err(|e| Error::Protocol(protocol_err(e)))
    }

    async fn read_bounded(&self, res: reqwest::Response) -> Result<String, Error> {
        if let Some(len) = res.content_length() {
            if len as usize > self.policy.max_body_bytes {
                return Err(Error::Transport(format!(
                    "response is {len} bytes, over the {} byte cap",
                    self.policy.max_body_bytes
                )));
            }
        }
        let bytes = res
            .bytes()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if bytes.len() > self.policy.max_body_bytes {
            return Err(Error::Transport(format!(
                "response is {} bytes, over the {} byte cap",
                bytes.len(),
                self.policy.max_body_bytes
            )));
        }
        String::from_utf8(bytes.to_vec())
            .map_err(|_| Error::Transport("response body is not UTF-8".into()))
    }
}

fn protocol_err(e: serde_json::Error) -> stepd_proto::EnvelopeError {
    stepd_proto::EnvelopeError::BadVersion(e.to_string())
}

#[async_trait]
impl Transport for HttpTransport {
    async fn deliver(
        &self,
        target: &AppTarget,
        attempt: &Attempt,
    ) -> Result<AttemptResponse, Error> {
        let client = self.approved_client(&target.url)?;

        let body = serde_json::to_string(attempt)?;
        let ts = chrono::Utc::now().timestamp();
        let nonce = uuid::Uuid::new_v4().to_string();

        let mut req = client
            .post(&target.url)
            .header("content-type", "application/json")
            .header(stepd_proto::HEADER_PROTOCOL, PROTOCOL_VERSION)
            .header(stepd_proto::HEADER_RUN_ID, attempt.run.id.to_string())
            .header(stepd_proto::HEADER_ATTEMPT, attempt.attempt.to_string())
            .header(stepd_proto::HEADER_FENCE, attempt.fence.to_string());

        if let Some(key) = target.keys.first() {
            req = req
                .header(
                    stepd_proto::HEADER_SIGNATURE,
                    sig::sign(key, &body, ts, &nonce),
                )
                .header(stepd_proto::HEADER_NONCE, nonce);
        }

        let res = req
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;

        let status = res.status().as_u16();
        // Every status the protocol assigns a distinct meaning to (§2.2). The
        // dispatcher's reaction differs by class, so collapsing them would turn
        // a deploy in progress into a permanently failed run.
        match status {
            200 => {}
            400 => {
                let detail = self.read_bounded(res).await.unwrap_or_default();
                return Err(Error::Config(format!(
                    "app rejected the attempt request as malformed: {detail}"
                )));
            }
            401 => return Err(Error::Transport("app rejected our signature (401)".into())),
            404 => {
                return Err(Error::Transport(format!(
                    "app does not host function '{}' (404)",
                    attempt.run.function_id
                )))
            }
            409 => {
                // The app noticed the fence was stale before we did. Nothing to
                // commit and nothing wrong.
                return Err(Error::StaleFence(attempt.run.id));
            }
            429 => {
                let retry_after = res
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unspecified")
                    .to_string();
                return Err(Error::Transport(format!(
                    "app is throttling; retry-after {retry_after}"
                )));
            }
            s => {
                let detail = self.read_bounded(res).await.unwrap_or_default();
                return Err(Error::Transport(format!("app returned {s}: {detail}")));
            }
        }

        // Verify the app's signature on the way back. Signing is required in both
        // directions: an unverified response is an unauthenticated instruction to
        // mutate durable run state.
        let sig_header = res
            .headers()
            .get(stepd_proto::HEADER_SIGNATURE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = self.read_bounded(res).await?;

        if !target.keys.is_empty() {
            match sig_header {
                Some(h) => {
                    sig::verify(&target.keys, &h, &body, ts, sig::DEFAULT_TOLERANCE_SECS)
                        .map_err(|e| Error::Transport(format!("app response signature: {e}")))?;
                }
                None => {
                    warn!(
                        run = %attempt.run.id,
                        "app response carried no signature; accepting it because the SDK may \
                         predate response signing, but this should be alerted on"
                    );
                }
            }
        }

        let parsed: AttemptResponse = serde_json::from_str(&body).map_err(|e| {
            Error::Transport(format!(
                "app returned a body that is not an op envelope: {e}"
            ))
        })?;
        parsed.validate()?;
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn cloud_metadata_is_denied_even_when_private_ranges_are_allowed() {
        // The single most important line in this crate. Every other rule can be
        // relaxed by an operator who knows their network; this one cannot,
        // because the cost of being wrong is every credential the server holds.
        let p = EgressPolicy {
            allow_private: true,
            allow_loopback: true,
            ..Default::default()
        };
        assert!(p.allows(ip("169.254.169.254")).is_err());
        assert!(p.allows(ip("fd00:ec2::254")).is_err());
    }

    #[test]
    fn link_local_loopback_and_private_are_denied_by_default() {
        let p = EgressPolicy::default();
        assert!(p.allows(ip("169.254.1.1")).is_err(), "link-local");
        assert!(p.allows(ip("127.0.0.1")).is_err(), "loopback");
        assert!(p.allows(ip("10.0.0.1")).is_err(), "RFC 1918");
        assert!(p.allows(ip("192.168.1.1")).is_err(), "RFC 1918");
        assert!(p.allows(ip("172.16.0.1")).is_err(), "RFC 1918");
        assert!(p.allows(ip("100.64.0.1")).is_err(), "carrier-grade NAT");
        assert!(p.allows(ip("fc00::1")).is_err(), "IPv6 unique local");
        assert!(p.allows(ip("fe80::1")).is_err(), "IPv6 link-local");
        assert!(p.allows(ip("0.0.0.0")).is_err(), "unspecified");
    }

    #[test]
    fn a_public_address_is_allowed() {
        let p = EgressPolicy::default();
        assert!(p.allows(ip("93.184.216.34")).is_ok());
        assert!(p.allows(ip("2606:2800:220:1::1")).is_ok());
    }

    #[test]
    fn development_mode_opens_only_what_it_says() {
        let p = EgressPolicy::development();
        assert!(p.allows(ip("127.0.0.1")).is_ok());
        assert!(p.allows(ip("10.0.0.1")).is_ok());
        // …and still not this.
        assert!(p.allows(ip("169.254.169.254")).is_err());
    }

    #[test]
    fn only_http_and_https_are_dereferenced() {
        let p = EgressPolicy::development();
        for url in ["file:///etc/passwd", "gopher://x/", "ftp://x/"] {
            let e = p.resolve(url).unwrap_err();
            assert!(
                e.contains("scheme") || e.contains("invalid url"),
                "{url}: {e}"
            );
        }
    }

    #[test]
    fn a_loopback_url_is_refused_under_the_default_policy() {
        let p = EgressPolicy::default();
        assert!(p.resolve("http://127.0.0.1:8080/stepd").is_err());
        assert!(EgressPolicy::development()
            .resolve("http://127.0.0.1:8080/stepd")
            .is_ok());
    }

    #[test]
    fn the_allowlist_overrides_the_range_rules() {
        let p = EgressPolicy {
            allowlist: vec!["127.0.0.1".into()],
            ..Default::default()
        };
        assert!(
            p.resolve("http://127.0.0.1:8080/x").is_ok(),
            "an operator naming a host has made the decision explicitly"
        );
        assert!(
            p.resolve("http://10.0.0.1:8080/x").is_err(),
            "and only for that host"
        );
    }

    #[test]
    fn a_refused_url_never_produces_a_client() {
        // The rebinding defence only works if the client is *derived from* the
        // policy check rather than merely preceded by it. If a refused URL could
        // still yield a client, a later refactor could drop the check and nothing
        // would notice.
        let t = HttpTransport::new(Duration::from_secs(5), EgressPolicy::default()).unwrap();
        assert!(t.approved_client("http://169.254.169.254/").is_err());
        assert!(t.approved_client("http://127.0.0.1:1/").is_err());
        assert!(t.approved_client("file:///etc/passwd").is_err());
    }

    #[test]
    fn an_approved_url_yields_a_client_pinned_to_that_address() {
        let t = HttpTransport::new(Duration::from_secs(5), EgressPolicy::development()).unwrap();
        assert!(t.approved_client("http://127.0.0.1:8080/stepd").is_ok());
        // Cached, so the address is resolved once and reused rather than
        // re-resolved per request.
        assert!(t.approved_client("http://127.0.0.1:8080/stepd").is_ok());
        assert_eq!(t.pinned.lock().unwrap().len(), 1);
    }

    #[test]
    fn the_pin_cache_is_bounded() {
        let t = HttpTransport::new(Duration::from_secs(5), EgressPolicy::development()).unwrap();
        {
            let mut p = t.pinned.lock().unwrap();
            for i in 0..1100u16 {
                p.insert(
                    (format!("h{i}"), i, IpAddr::from([127, 0, 0, 1])),
                    reqwest::Client::new(),
                );
            }
        }
        let _ = t.approved_client("http://127.0.0.1:8080/x");
        assert!(
            t.pinned.lock().unwrap().len() < 1100,
            "an unbounded map keyed by tenant-supplied hostnames grows without limit"
        );
    }
}
