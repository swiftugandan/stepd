# ADR-003: Event envelope — CloudEvents 1.0 with `stepd*` extensions

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

Events are the boundary between stepd and everything else. They arrive from
customer services, from message brokers, from other people's SaaS webhooks, and
from stepd itself when a handler emits one. Whatever shape they take is a public
contract: it appears in the ingest API, in the attempt request handed to every
SDK, in the console's event explorer, and in every `wait_event` correlation for
as long as those runs live.

The default move is to invent a small envelope — `{type, key, payload}` — because
it fits the fields the engine actually reads. It also means every producer needs a
stepd-specific adapter, every consumer downstream of stepd needs another one, and
nothing off the shelf (a broker's CloudEvents binding, an OTel exporter, a
schema registry) can read a stepd event without translation.

Two fields the engine genuinely needs have no CloudEvents equivalent: the business
key that selects the keyed single-writer, and the idempotency key that ingest
deduplicates on.

## Decision

The event envelope is CloudEvents 1.0 in structured mode, JSON. `specversion`,
`id`, `source`, `type`, `time` and `data` carry their standard meanings. The two
engine-specific fields are CloudEvents **extension attributes**, named per the
specification's rules — lowercase, alphanumeric, no separators:

| Attribute | Meaning |
|---|---|
| `stepdkey` | Business key; overrides the function's `key_expr` |
| `stepdidempotency` | Ingest deduplication key |

Extensions rather than a nested `stepd` object, because CloudEvents attributes are
flat by design and a nested object would not survive a binding-mode conversion —
it would arrive as one opaque header and stop being addressable.

`stepdkey` overriding `key_expr` is deliberate: a producer that already knows the
business key should not have to encode it so that the consumer's CEL can
rediscover it. `engine/rust/crates/stepd-server/src/ingest.rs` prefers `event.key` and
falls back to evaluating the function's expression.

The engine never interprets `data`. Bulk content is a `$blob` or `$ref` payload
(ADR-010), so the envelope stays small enough to be worth standardising.

## Consequences

### What this makes easy

* Producers that already emit CloudEvents — brokers, gateways, other event-driven
  services — post to `/v1/events` with no adapter.
* The wire format is documented by someone else. `spec/PROTOCOL.md` says
  "CloudEvents 1.0" and inherits its rules for attribute naming, versioning and
  binding modes instead of restating them badly.
* An SDK author in another language has a library for the envelope and only needs
  to know two extension names.
* `id` + `source` gives a defined dedupe identity even when a producer omits
  `stepdidempotency`.

### What this makes hard

* Two names for one concept. The Rust field is `key`, the wire field is `stepdkey`;
  the mapping lives in `#[serde(rename)]` and in `From<EventIn>`, and it must be
  right in both directions or dedupe and keying quietly stop working.
* Adding an engine-level field later means adding an extension attribute, which is
  a public protocol change, not an internal one.
* Standardising the envelope creates an expectation of fidelity that the storage
  layer does not meet. `Event` in `stepd-proto` models only the attributes stepd
  uses, and the `events` table stores `id, ns, type, source, time, key, idem,
  subject_key, data` — so `datacontenttype`, `dataschema` and CloudEvents `subject`
  are dropped on ingest rather than round-tripped.

### What we accept

* stepd accepts events it would not itself emit: `specversion` defaults to `1.0`
  when a producer omits it and `id` is optional on the wire, though CloudEvents
  requires both. Rejecting a webhook that is 90% conformant would trade a real
  integration for a spec point.
* Unmodelled attributes are lost, not passed through. A consumer reading stepd's
  event log sees a conformant CloudEvent, but not necessarily the one that arrived.
* Only structured-mode JSON is supported today. Binary mode — attributes in HTTP
  headers — is a documented CloudEvents binding that stepd does not implement, so
  a producer using it needs a shim after all.
* The extension names are permanent. `stepdkey` cannot be renamed without breaking
  every stored event and every producer.

## Alternatives considered

| Option | Why not |
|---|---|
| A bespoke `{type, key, payload}` envelope | Smaller, and every producer and consumer then needs a stepd-specific adapter; nothing off the shelf can read the event log. |
| A nested `stepd: {key, idempotency}` object inside the envelope | CloudEvents attributes are flat; a nested object collapses to one opaque header under binding-mode conversion and stops being addressable. |
| Extensions named `stepd-key` / `stepd_key` | CloudEvents attribute names are lowercase alphanumeric with no separators; a hyphen or underscore is non-conformant and breaks header mapping. |
| Putting the key inside `data` | Makes the engine interpret the payload, which contradicts the control-plane rule that stepd never reads application bytes. |
| Requiring every producer to send `stepdidempotency` | Most webhook sources cannot. `id` + `source` is the fallback identity, so dedupe degrades rather than disappears. |

## Verification

* `spec/rust/crates/stepd-proto/src/types.rs` defines `Event` with
  `#[serde(rename = "type")]` on `event_type`, `#[serde(rename = "stepdkey")]` on
  `key` and `#[serde(rename = "stepdidempotency")]` on `idempotency`, and defaults
  `specversion` to `1.0`. Its test `event_uses_cloudevents_field_names` asserts the
  serialised form carries `specversion: "1.0"` and `type`, and explicitly asserts
  that no `event_type` field appears — a rename that silently reverted would
  otherwise produce an envelope no CloudEvents consumer recognises.
* `engine/rust/crates/stepd-server/src/ingest.rs` defines the wire-side `EventIn` and the
  `From<EventIn> for stepd_proto::Event` conversion.
  `a_cloudevent_maps_onto_the_wire_type_without_losing_extensions` round-trips a
  full envelope and asserts both extensions survive, with the comment that losing
  the idempotency key silently disables dedupe;
  `an_event_without_the_optional_fields_still_parses` pins the tolerant defaults.
* The override rule is implemented in `start_matching_runs` in the same file, which
  takes `event.key` when present and calls `evaluate_key` otherwise.
* `engine/rust/crates/stepd-core/tests/engine.rs::idempotent_ingest_returns_the_same_event_id`
  and `engine/rust/crates/stepd-server/tests/end_to_end.rs::a_duplicate_event_does_not_start_a_second_run`
  assert that the dedupe key does what the extension exists for, in-memory and
  against a real database respectively.
