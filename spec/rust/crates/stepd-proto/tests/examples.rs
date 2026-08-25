//! Committed examples are the documents the schemas and this crate must both
//! accept. `spec/validate.py` is the schema half; this is the serde half.
//!
//! A document that validates and then fails to parse here — or that this crate
//! emits and the schemas reject — is the drift that previously survived because
//! each side only checked itself. Parse is this file; emit is `schema.rs`.

use std::fs;
use std::path::PathBuf;
use stepd_proto::{
    AppManifest, Attempt, AttemptResponse, BlobReserveRequest, BlobReserveResponse,
    ConformanceManifest, FunctionConfig, ProblemBody,
};

#[test]
fn the_remaining_wire_examples_parse() {
    let app: AppManifest = serde_json::from_str(&read("app-manifest.example.json"))
        .expect("app-manifest.example.json");
    app.validate()
        .expect("app-manifest.example.json must be semantically valid");
    let f: FunctionConfig = serde_json::from_str(&read("function-config-cron.example.json"))
        .expect("function-config-cron.example.json");
    f.validate()
        .expect("function-config-cron.example.json must be semantically valid");
    let _: ConformanceManifest = serde_json::from_str(&read("conformance-manifest.example.json"))
        .expect("conformance-manifest.example.json");
    let _: BlobReserveRequest = serde_json::from_str(&read("blob-reserve-request.example.json"))
        .expect("blob-reserve-request.example.json");
    let _: BlobReserveResponse = serde_json::from_str(&read("blob-reserve-response.example.json"))
        .expect("blob-reserve-response.example.json");
    let dedup: BlobReserveResponse =
        serde_json::from_str(&read("blob-reserve-response-dedup.example.json"))
            .expect("blob-reserve-response-dedup.example.json");
    assert!(dedup.deduplicated);
    assert!(dedup.upload_url.is_none());
    let _: ProblemBody =
        serde_json::from_str(&read("problem.example.json")).expect("problem.example.json");
}

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../examples")
}

fn read(name: &str) -> String {
    fs::read_to_string(examples_dir().join(name))
        .unwrap_or_else(|e| panic!("could not read spec/examples/{name}: {e}"))
}

#[test]
fn the_attempt_request_example_parses() {
    let attempt: Attempt = serde_json::from_str(&read("attempt-request.example.json"))
        .expect("attempt-request.example.json must deserialize as Attempt");
    assert_eq!(attempt.protocol, stepd_proto::PROTOCOL_VERSION);
    assert_eq!(attempt.fence, 4);
    assert_eq!(
        attempt.run.lineage_id.to_string(),
        attempt.run.id.to_string()
    );
    assert_eq!(attempt.run.key.as_deref(), Some("order:4711"));
    assert_eq!(attempt.events[0].key.as_deref(), Some("order:4711"));
    assert_eq!(attempt.events[0].idempotency.as_deref(), Some("4711"));
    let v = serde_json::to_value(&attempt).expect("re-serialise");
    assert_eq!(v["events"][0]["stepdkey"], "order:4711");
    assert_eq!(v["events"][0]["stepdidempotency"], "4711");
    assert!(
        v["events"][0].get("key").is_none(),
        "the wire name is stepdkey; a `key` field here would be ignored on ingest"
    );
}

#[test]
fn every_attempt_response_example_parses_and_validates() {
    let dir = examples_dir();
    let mut names: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().into_string().unwrap())
        .filter(|n| n.starts_with("attempt-response-") && n.ends_with(".example.json"))
        .collect();
    names.sort();
    assert!(
        names.len() >= 8,
        "expected the committed response examples, found {names:?}"
    );

    for name in names {
        let envelope: AttemptResponse = serde_json::from_str(&read(&name))
            .unwrap_or_else(|e| panic!("{name} must deserialize as AttemptResponse: {e}"));
        envelope
            .validate()
            .unwrap_or_else(|e| panic!("{name} must be a well-formed envelope: {e}"));
    }
}

#[test]
fn a_crate_constructed_attempt_serialises_a_numeric_fence() {
    // The inverse of the example lattice: what this crate emits must be a
    // number, because that is what the schema now requires and what the server
    // sends. A ULID string here would be the old published spec leaking back in.
    let raw = read("attempt-request.example.json");
    let attempt: Attempt = serde_json::from_str(&raw).unwrap();
    let v = serde_json::to_value(&attempt).unwrap();
    assert!(
        v["fence"].is_number(),
        "fence must serialise as a JSON number, got {}",
        v["fence"]
    );
    assert!(v.get("logs").is_none(), "logs is not a wire field");
}

#[test]
fn a_crate_constructed_envelope_never_serialises_join_or_logs() {
    let envelope = AttemptResponse::single(stepd_proto::Op::Done { data: None });
    let v = serde_json::to_value(&envelope).unwrap();
    assert!(v.get("join").is_none());
    assert!(v.get("logs").is_none());
    assert!(v.get("diagnostics").is_none());
    assert_eq!(v["orphaned_steps"], 0);
}
