//! The other half of the example lattice in `examples.rs`.
//!
//! That file asks whether this crate *accepts* the committed documents.
//! This one asks whether a document this crate *emits* is one the published
//! schemas still accept. serde `skip_serializing_if`, a renamed field, or a
//! default that vanished would pass every parse test and fail here.
//!
//! The schema engine is a test-only dependency. It does not retrieve over the
//! network: every `$ref` is resolved from `spec/schemas/`.

use chrono::{TimeZone, Utc};
use jsonschema::Registry;
use serde::Serialize;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;
use stepd_proto::{
    AppManifest, Attempt, AttemptResponse, BlobReserveRequest, BlobReserveResponse,
    ConformanceManifest, ConformanceSuite, ErrorBody, Event, FunctionConfig, Op, ProblemBody,
    StaticHazard, Trigger,
};
use uuid::Uuid;

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../schemas")
}

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../examples")
}

fn read_example(name: &str) -> String {
    fs::read_to_string(examples_dir().join(name))
        .unwrap_or_else(|e| panic!("could not read spec/examples/{name}: {e}"))
}

fn registry() -> &'static Registry<'static> {
    static REGISTRY: OnceLock<Registry<'static>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut pairs: Vec<(String, Value)> = Vec::new();
        for entry in fs::read_dir(schemas_dir()).expect("spec/schemas") {
            let path = entry.expect("schema dir entry").path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if !name.ends_with(".schema.json") {
                continue;
            }
            let doc: Value = serde_json::from_str(
                &fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {name}: {e}")),
            )
            .unwrap_or_else(|e| panic!("{name} is not JSON: {e}"));
            if let Some(id) = doc.get("$id").and_then(|v| v.as_str()) {
                pairs.push((id.to_string(), doc.clone()));
            }
            pairs.push((name.to_string(), doc));
        }
        assert!(
            !pairs.is_empty(),
            "expected published schemas under spec/schemas/"
        );
        Registry::new()
            .extend(pairs)
            .expect("schema $id is not a URI")
            .prepare()
            .expect("schema registry failed to prepare")
    })
}

fn schema_uri(spec: &str) -> String {
    if spec.starts_with("https://") {
        spec.into()
    } else {
        format!("https://stepd.dev/schemas/v1/{spec}")
    }
}

fn assert_schema(spec: &str, instance: &Value) {
    let uri = schema_uri(spec);
    let schema = json!({ "$ref": uri });
    let validator = jsonschema::options()
        .with_registry(registry())
        .build(&schema)
        .unwrap_or_else(|e| panic!("could not compile {uri}: {e}"));
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|e| format!("{e}"))
        .collect();
    assert!(
        errors.is_empty(),
        "a document this crate emitted was rejected by {uri}:\n  {}\ninstance: {}",
        errors.join("\n  "),
        serde_json::to_string_pretty(instance).unwrap()
    );
}

fn round_trip<T>(raw: &str) -> Value
where
    T: serde::de::DeserializeOwned + Serialize,
{
    let value: T = serde_json::from_str(raw).expect("example must parse as the proto type");
    serde_json::to_value(&value).expect("parsed value must serialise")
}

const RUN: &str = "01926f5a-1c40-7c9a-9f3e-2b7d4e6a8c10";
const HASH: &str = "3f2a91c4b70e1d55";
const DIGEST: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn run_id() -> Uuid {
    RUN.parse().unwrap()
}

fn until() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 26, 10, 0, 0).unwrap()
}

fn function(id: &str, trigger: Trigger) -> FunctionConfig {
    FunctionConfig {
        id: id.into(),
        version: None,
        name: None,
        description: None,
        triggers: vec![trigger],
        key_expr: None,
        idempotency_expr: None,
        priority_expr: None,
        concurrency: None,
        rate_limit: None,
        debounce: None,
        batch: None,
        retries: None,
        timeouts: None,
        cancel_on: None,
        on_failure: None,
        singleton: false,
        input_schema: None,
        output_schema: None,
        inbox: None,
        limits: None,
        on_cancel: false,
    }
}

/// Same mapping as `spec/validate.py` CASES: a round-trip through this crate
/// must still be a document those schemas accept.
#[test]
fn a_round_tripped_example_is_still_schema_valid() {
    let cases: &[(&str, &str, fn(&str) -> Value)] = &[
        (
            "attempt-request.schema.json",
            "attempt-request.example.json",
            round_trip::<Attempt>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-parallel.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-wait.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-done.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-error.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-media.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-continue.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-batch.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "attempt-response.schema.json",
            "attempt-response-partial-failure.example.json",
            round_trip::<AttemptResponse>,
        ),
        (
            "app-manifest.schema.json",
            "app-manifest.example.json",
            round_trip::<AppManifest>,
        ),
        (
            "function-config.schema.json",
            "function-config-cron.example.json",
            round_trip::<FunctionConfig>,
        ),
        (
            "conformance-manifest.schema.json",
            "conformance-manifest.example.json",
            round_trip::<ConformanceManifest>,
        ),
        (
            "blob-reserve.schema.json#/$defs/request",
            "blob-reserve-request.example.json",
            round_trip::<BlobReserveRequest>,
        ),
        (
            "blob-reserve.schema.json#/$defs/response",
            "blob-reserve-response.example.json",
            round_trip::<BlobReserveResponse>,
        ),
        (
            "blob-reserve.schema.json#/$defs/response",
            "blob-reserve-response-dedup.example.json",
            round_trip::<BlobReserveResponse>,
        ),
        (
            "problem.schema.json",
            "problem.example.json",
            round_trip::<ProblemBody>,
        ),
    ];

    for (schema, example, through) in cases {
        let emitted = through(&read_example(example));
        assert_schema(schema, &emitted);
    }
}

#[test]
fn a_crate_constructed_envelope_is_schema_valid() {
    let done = AttemptResponse::single(Op::Done {
        data: Some(json!({"shipped": true})),
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&done).unwrap(),
    );

    let sleep = AttemptResponse::single(Op::Sleep {
        id: "cooldown".into(),
        hash: HASH.into(),
        until: until(),
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&sleep).unwrap(),
    );

    let wait = AttemptResponse::single(Op::WaitEvent {
        id: "approval".into(),
        hash: HASH.into(),
        event: "order.approved".into(),
        since: "run_start".into(),
        timeout_at: Some(until()),
        prompt: None,
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&wait).unwrap(),
    );

    let err = AttemptResponse::single(Op::Error {
        retryable: true,
        step: Some("charge".into()),
        error: ErrorBody::coded("gateway_down", "upstream 503"),
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&err).unwrap(),
    );

    let batch = AttemptResponse::batch(vec![
        Op::Step {
            id: "ship".into(),
            hash: "1122334455667788".into(),
            data: Some(json!({"ok": true})),
            meta: None,
            error: None,
        },
        Op::Invoke {
            id: "invoice".into(),
            hash: "aabbccddeeff0011".into(),
            function: "billing-issue".into(),
            input: Some(json!({"order": 4711})),
            detach: false,
        },
    ]);
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&batch).unwrap(),
    );

    let signal = AttemptResponse::single(Op::Signal {
        id: "nudge".into(),
        hash: HASH.into(),
        target_run: run_id(),
        event: Event::new(
            "order.nudge",
            "/fn/order-fulfilment",
            json!({"order_id": 4711}),
        ),
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&signal).unwrap(),
    );

    let successor = AttemptResponse::single(Op::ContinueAsNew {
        id: "next-cycle".into(),
        hash: HASH.into(),
        input: Some(json!({"cursor": 41200})),
    });
    assert_schema(
        "attempt-response.schema.json",
        &serde_json::to_value(&successor).unwrap(),
    );
}

#[test]
fn a_crate_constructed_registration_document_is_schema_valid() {
    let f = function(
        "order-fulfilment",
        Trigger::Event {
            event: "order.created".into(),
            expr: Some("event.data.total > 0".into()),
        },
    );
    f.validate().unwrap();
    assert_schema(
        "function-config.schema.json",
        &serde_json::to_value(&f).unwrap(),
    );

    let cron = function(
        "nightly-billing",
        Trigger::Cron {
            cron: "0 3 * * *".into(),
            tz: "Europe/London".into(),
            catchup: Some(stepd_proto::CatchUp::All),
            catchup_limit: Some(7),
            misfire_window: Some("PT12H".into()),
            singleton: true,
            run_key: Some("billing:nightly".into()),
        },
    );
    cron.validate().unwrap();
    assert_schema(
        "function-config.schema.json",
        &serde_json::to_value(&cron).unwrap(),
    );

    let manifest = AppManifest {
        protocol: stepd_proto::PROTOCOL_VERSION.into(),
        app_id: "billing".into(),
        url: "https://billing.internal/stepd".into(),
        sdk: Some("rust/0.1.0".into()),
        checksum: Some(format!("sha256:{DIGEST}")),
        env: Some("prod".into()),
        capabilities: Some(vec![stepd_proto::Capability::Parallel]),
        functions: vec![f],
    };
    manifest.validate().unwrap();
    assert_schema(
        "app-manifest.schema.json",
        &serde_json::to_value(&manifest).unwrap(),
    );

    let conformance = ConformanceManifest {
        protocol: stepd_proto::PROTOCOL_VERSION.into(),
        sdk: Some("rust/0.1.0".into()),
        suites: vec![ConformanceSuite::Memoization, ConformanceSuite::Signature],
        statically_prevented: vec![StaticHazard::OffpathClaim],
    };
    assert_schema(
        "conformance-manifest.schema.json",
        &serde_json::to_value(&conformance).unwrap(),
    );
}

#[test]
fn a_crate_constructed_reserve_and_problem_are_schema_valid() {
    let req = BlobReserveRequest {
        run_id: run_id(),
        step_id: Some("thumbnail".into()),
        size: 184320,
        sha256: DIGEST.into(),
        content_type: Some("image/png".into()),
        filename: Some("thumb.png".into()),
    };
    assert_schema(
        "blob-reserve.schema.json#/$defs/request",
        &serde_json::to_value(&req).unwrap(),
    );

    let upload = BlobReserveResponse {
        blob_id: "01926f5a-2200-7aaa-8000-0123456789ab".parse().unwrap(),
        deduplicated: false,
        upload_url: Some("https://blobs.example.com/x".into()),
        method: Some("PUT".into()),
        headers: None,
        expires_at: Some(until()),
        relay: false,
    };
    assert_schema(
        "blob-reserve.schema.json#/$defs/response",
        &serde_json::to_value(&upload).unwrap(),
    );

    let dedup = BlobReserveResponse {
        blob_id: "01926f5a-2200-7aaa-8000-0123456789ab".parse().unwrap(),
        deduplicated: true,
        upload_url: None,
        method: None,
        headers: None,
        expires_at: None,
        relay: false,
    };
    assert_schema(
        "blob-reserve.schema.json#/$defs/response",
        &serde_json::to_value(&dedup).unwrap(),
    );

    let problem = ProblemBody {
        problem_type: "about:blank".into(),
        title: "Bad request".into(),
        status: 400,
        detail: Some("cursor is not a uuid".into()),
        instance: None,
        code: Some("bad_cursor".into()),
        run_id: None,
        op: None,
    };
    assert_schema(
        "problem.schema.json",
        &serde_json::to_value(&problem).unwrap(),
    );
}
