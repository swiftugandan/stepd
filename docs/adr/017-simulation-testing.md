# ADR-017: Deterministic simulation testing as the primary correctness technique

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-23 |
| Supersedes | — |

## Context

The defects this engine exists to prevent are not the ones a test suite normally
catches. They need a specific interleaving — a signal delivered between a wait
being registered and the commit that records it, a lease expiring while an
attempt is in flight, a duplicate commit arriving after a crash — and a
conventional test reaches those by luck, if ever. Worse, they fail silently: the
op is accepted, acknowledged and lost, and nothing throws. Gap F1 recorded that
no technique in the plan was capable of finding them, and PRD §10.1 puts
detectability at the centre of the risk model for exactly this reason.

The countermeasure must explore interleavings nobody enumerated, state what
"correct" means as machine-checkable properties so a violation is an assertion
failure rather than a judgement call, and make a failure reproducible from a
single number — a race condition you cannot re-run is a rumour, not a finding.

## Decision

**Deterministic simulation testing is the primary correctness technique for
zone-R1 code**, ahead of unit and integration tests, which cover the same code
only incidentally.

* **Nine properties**, from PRD §10.3, asserted continuously during a run and
  again at quiescence: P1 no lost effect, P2 no duplicate record, P3 no lost
  signal, P4 hash stability, P5 keyed exclusivity, P6 fence monotonicity, P7
  chain continuity, P8 cascade completeness, P9 termination. A property is the
  deliverable; the seeds are just the search.
* **Eleven fault types**, combined by **swarm testing**: each seed enables a
  random *subset*, never all of them. Enabling everything sounds more thorough
  and is not — with every fault active, runs die before the deep interleavings
  have a chance to form, so uniform generation systematically under-explores the
  states that matter most.
* **A hand-rolled PRNG.** Depending on a crate's generator means a failing seed
  stops reproducing the day that crate changes its algorithm, and "here is the
  seed" is the entire value of the technique. Nearby seeds are decorrelated by
  SplitMix64 seeding, so a sweep of `1..=N` explores N neighbourhoods rather than
  one neighbourhood N times.
* **Positive controls are mandatory.** Without one, "P4 never fired" and "P4 is
  vacuous" are the same observation, and a suite of vacuous properties produces
  confidence with nothing behind it.
* **Targeted tests where the fuzzer provably cannot reach.** Coverage reporting
  zero hits is a finding, not noise: it means either a missing generator or an
  unreachable rule, and those need opposite responses.
* **The harness drives the real engine**, not only a model. The model finds
  design gaps; only real SQL against real PostgreSQL finds implementation ones.

## Consequences

### What this makes easy
* An ordering bug becomes a number. `seed 317` re-runs the exact interleaving, so
  fixing it is ordinary debugging rather than archaeology.
* Every historical failing seed becomes a permanent fixed-seed case, and the
  budget is a parameter rather than a rewrite: `STEPD_SIM_SEEDS` moves the same
  harness between the per-commit tier and a nightly soak.
* Flakiness stops being a category. A failure carries a seed, so an intermittent
  red is nearly always a real rare interleaving (PRD §10.6).

### What this makes hard
* Properties are production code and must be reviewed as such. A subtly wrong
  property is worse than a missing one, because it certifies false safety.
* Passing is not exercising. `reference/coverage_check.py` showed cascade
  cancellation hit zero times across 500 green seeds — the suite had never
  touched a fix that had just been made. Green needs coverage evidence beside it.
* Some states are unreachable by construction. With blocking `invoke`, a live
  non-detached child at `continue_as_new` cannot occur, so the rule guarding it
  must be tested directly rather than left to a fuzzer that provably cannot
  produce the state.

### What we accept
* Simulation proves properties over the interleavings it explored, not over all
  of them. It is a search, not a proof; the stop rule in PRD §10.3 moves budget
  rather than growing it when a zone stops yielding findings.
* The Rust harness needs a live PostgreSQL. Without `STEPD_TEST_DATABASE_URL` it
  skips, which means it can be green because it did not run.

## Alternatives considered

| Option | Why not |
|---|---|
| More integration tests | They exercise the happy interleaving repeatedly. The R1 defects need an interleaving nobody wrote down, and no amount of scripted concurrency reaches it reliably. |
| Property tests over pure functions only | Hashing and backoff are already covered there, and neither is where the silent corruption lives — it lives in the commit transaction's interaction with concurrent delivery. |
| A formal TLA+ model alone | Finds design defects, which is exactly what the Python model did (A8). It cannot find that `signal` bypassed `deliver_to_inbox` in the shipped SQL, because that code is not in the model. |
| Enable every fault on every seed | Runs die too early; the deep interleavings never form. This is the specific failure swarm testing exists to avoid. |
| Use `rand` / `proptest` generators | A seed would stop meaning the same thing across a dependency upgrade, destroying reproducibility — the one property the whole technique rests on. |
| Trust green properties without positive controls | "Never fired" and "cannot fire" are the same observation from outside. |

## Verification

* `reference/simulation.py` — the Python model. Declares P1–P9 in its module
  docstring, `FEATURES` lists the eleven fault types, and `simulate()` selects a
  random subset per seed (`k = rng.randint(2, len(FEATURES))`). Two controls run
  before the sweep and abort it on failure: `test_p4_catches_nondeterminism`,
  which feeds P4 a handler whose step id depends on attempt count, and
  `test_continue_as_new_rejects_live_children`, the targeted test for the state
  the fuzzer cannot reach. This harness found gap A8 — `continue_as_new`
  orphaning live children — on its first run, via P8. The README records 120,000
  seeds with zero violations.
* `rust/crates/stepd-store-postgres/tests/simulation.rs` — the same nine
  properties, the same eleven `FAULTS`, and the same swarm scheme, driving the
  **real** store against real PostgreSQL. Its module docstring states why the
  model is not enough: the three defects that actually shipped (a signal relayed
  into the inbox without waking the run, a wait whose timeout was never
  scheduled, a cascade that committed per level) were implementation defects, and
  a model of the design is blind to every one.
* Tests in that file: `no_property_is_violated_across_the_seed_budget` (budget
  from `STEPD_SIM_SEEDS`, default 40), `p4_fires_on_a_nondeterministic_handler`
  (the positive control), `the_rng_is_reproducible_and_not_obviously_biased`
  (asserts `seq(42) == seq(42)`, `seq(1)[0] != seq(2)[0]`, and a bucket
  distribution check), and `swarm_subsets_are_proper_subsets`.
* **Not verified:** the Rust harness returns early with `SKIPPED` unless
  `STEPD_TEST_DATABASE_URL` is set, and the README's provenance caveat states
  this workspace has not been run against a live database. The properties and the
  harness are written; the evidence of 120,000 clean seeds belongs to the Python
  model, not to this Rust tree.
