# stepd SDK mechanism prototype

Working prototype of the two R1 mechanisms described in `SDK-DESIGN-rust.md`:
short-circuit control flow and occurrence assignment.

Deliberately synchronous: the mechanisms are runtime-independent, and removing
scheduling noise makes the properties exhaustively testable.

```bash
cargo test                      # 15 property + adversarial tests
cargo run --example eager_claim # demonstrates eager vs lazy hash claiming
```

| File | Contains |
|---|---|
| `src/lib.rs` | `Ctx`, claim/memo/halt machinery, `parallel`, a driver that simulates the server |
| `tests/properties.rs` | Memoization, loops, determinism, parallelism, versioning |
| `tests/adversarial.rs` | Concurrent claims, 50-replay hash stability, 500 seeded crash interleavings |
| `examples/eager_claim.rs` | Why `ctx.step()` must claim eagerly, not inside the returned future |

The last example is the load-bearing finding: claiming the occurrence when `step()` is
*called* rather than when its future is *polled* makes `join!` safe by construction.
Lazy claiming ties the hash to scheduler order, which silently re-executes completed work.
