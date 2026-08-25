# stepd TypeScript SDK — Design

The companion to [`SDK-DESIGN-rust.md`](SDK-DESIGN-rust.md), covering the same
ground for TypeScript. Where the two differ deliberately, the decision and its
reason are in [ADR-025](adr/025-typescript-sdk-divergences.md) and are not
repeated here.

Read §2 and §3 first. Everything else is ordinary engineering; those two carry
all of this SDK's risk, because their failure modes corrupt data **silently**
rather than erroring.

---

## 1. Surface

```ts
import { App, fn, retryable, fatal, type Ctx } from '@stepd/sdk';

const app = new App({ appId: 'billing', url: 'https://billing.internal/stepd' })
  .signingKey(process.env.STEPD_SIGNING_KEY!)
  .function(
    fn('order-fulfilment')
      .onEvent('order.created')
      .key("'order:' + string(event.data.order_id)")
      .run(async (ctx: Ctx) => {
        const tx = await ctx.step('charge', () => gateway.charge());
        await ctx.sleep('cooldown', 86_400_000);
        const approval = await ctx.waitEvent('approval', 'order.approved', {
          timeoutMs: 7 * 86_400_000,
        });
        return { tx, approval };
      }),
  );
```

Served through whatever runtime is to hand. The core is
`(Request) => Promise<Response>`:

```ts
import { createHandler } from '@stepd/sdk';        // Bun, Deno, Workers, Next.js
import { nodeListener } from '@stepd/sdk/node';    // node:http
```

Three packages, split by **failure mode** rather than by layer:

| | Owns | Fails |
|---|---|---|
| [`@stepd/protocol`](../spec/typescript) | Wire types, `stepHash`, `sign`/`verify`, envelope validation | Silently, if the hash or the signature is wrong |
| `@stepd/sdk-core` | `Ctx`, claiming, memo, `Halt`, `runPass`, `ctx.parallel` | Silently |
| `@stepd/sdk` | Builders, manifest, handler, nonce cache, blobs, paging, harness | Visibly |

`@stepd/protocol` lives with the specification rather than inside the SDK,
because the engine depends on it too ([ADR-024](adr/024-language-trees.md)).

## 2. Occurrence assignment — claim eagerly

### 2.1 The rule

**`ctx.step(id, fn)` claims its occurrence when it is called, not when the
returned value is awaited.** It is a synchronous function returning a
`PromiseLike`, never an `async function`.

### 2.2 What goes wrong otherwise

The hash is the memo key. If a completed step's hash differs between attempts,
the server holds a result the handler never asks for and the handler asks for one
the server does not have. The step re-executes — the payment is taken twice — and
the old result is orphaned. **Nothing errors.** The run completes, the console
shows green, and the only trace is an orphaned-hash count nobody was reading.

Under lazy claiming, occurrence follows scheduler order. Under eager claiming it
follows declaration order, which is a property of the source text and therefore
identical on every attempt.

Because the claim is already done by the time anything is awaited,
`Promise.all([a, b])` over steps is safe. Users are not one forgotten import away
from silent corruption. `ctx.parallel` adds two things eager claiming cannot do
by itself: it refuses a repeated id, and it emits every member in one envelope
rather than one round trip each.

### 2.3 Defence in depth

Rust makes `Ctx` `!Send`, so the mistake fails to compile. There is no equivalent
here, so three runtime guards are the whole defence:

1. **Pass sealing.** `runPass` seals the `Ctx` once the handler settles. A claim
   after that — from a timer, a floating promise, a captured closure — is
   `Halt.fatal`, never a guessed hash.
2. **Step-body depth.** A claim made inside a step body is refused: it happens
   only on the attempts where that body runs, so its occurrence depends on what
   was memoised.
3. **Group uniqueness.** `ctx.parallel` refuses a repeated id *before any
   member's body has run* — possible only because a `StepFuture` is lazy, so
   there is nothing a rejection would have to undo.

`test/claiming.test.ts` demonstrates the property rather than asserting it: one
test awaits the same steps in opposite orders and shows the hashes unchanged, and
a second implements lazy claiming as a deliberate control and asserts that it
*does* break. If the control ever passes, the first test proves nothing.

The control is worth a note of its own. The obvious way to write it — an `async`
function called eagerly — does not reproduce the bug at all, because both bodies
are already running and resume in FIFO order however you await them. Modelling
"claim at await" needs a thunk that claims on subscription. A control that
quietly fails to model the hazard is worse than no control.

## 3. Short-circuit control flow

A handler has to stop mid-function once a step is recorded, so the server can
commit it.

| Mechanism | Verdict |
|---|---|
| Generators (`function*`) | Rejected. Forces every workflow into an unfamiliar shape and loses `async`/`await` ergonomics entirely. |
| A promise that never settles | Rejected. Leaks the pending work and gives the caller no way to return the op. |
| A returned discriminated union checked at each step | Rejected. Every call site becomes an `if`, and one forgotten check is a silent skip. |
| **A thrown sentinel** | **Chosen.** |

`throw` across an `await` is well-defined in JavaScript, unlike unwinding across
an async boundary in Rust — which is why the Rust design doc rejects `panic!` and
this one does not.

The ops do **not** travel in the thrown value. They accumulate on the `Ctx` and
are drained once, after the pass, in every arm of `runPass` — including the error
arms. An earlier Rust version drained only in the yield arm, so a pass that
recorded three steps and then failed committed none of them (ADR-023).

**The swallowed-halt problem is worse here than in Rust.** `catch` is idiomatic
JavaScript, and two shapes absorb a `Halt` without looking like mistakes:
`try { … } catch {}`, and `Promise.allSettled`, which never rejects. Both are
caught: `Ctx` records that a step was reached, and a handler that returns
normally while halted is a non-retryable `swallowed_halt`.

## 4. Replay pass

One attempt is one call of the handler from the top.

* `ctx.step` claims, then consults the memo. A hit returns the recorded value and
  **never calls the body**. A miss runs it, records an op, and throws
  `Halt('yield')`.
* A recorded step that decodes into a shape the handler no longer expects is a
  non-retryable error naming the step — never a silent re-execution. Its side
  effect already happened.
* `waitEvent` does not share the step replay path. A timeout is an outcome, not
  an error: "nobody approved in seven days" resolves to `null` for the handler to
  branch on. Reusing the step path here failed `conf-wait-timeout` and was caught
  by the conformance battery rather than by 76 unit tests.
* A **terminal** step failure is recorded; a **retryable** one is not. A recorded
  failure is memoised, so recording a retryable one means the retry replays the
  error instead of the body and never happens.
* A retryable failure never travels with recorded ops (§5.2.2). The work goes
  now, alone, and the failure is raised again next attempt where the work is
  memoised. This terminates because each pass records strictly less.

## 5. Deadline discipline

§7.1.1: an SDK **must not** cancel or drop a running step to meet the attempt
deadline. In Node that means the pass is not tied to the request's `AbortSignal`
and a client disconnect does not cancel it — the promise settles regardless and
the HTTP response is abandoned instead. The server treats no response as
`unknown` and retries.

`nodeListener` documents that a `server.requestTimeout` set elsewhere breaks the
guarantee silently. `conf-abandon` exercises it: on its first attempt the step
body never resolves, and the case asserts the step re-executes and is recorded
once.

## 6. Payloads

Inline JSON under 1 MiB; a managed blob to 100 MiB; a `$ref` above that, which
the server never dereferences.

`Blobs` is the two-phase client: reserve against the server, `PUT` straight to
the store replaying every reservation header **verbatim and exactly once** — a
presigning backend signs them, and a duplicate `content-length` breaks SigV4 as
thoroughly as a missing header — then return a reference with **no `url`**. Read
URLs are minted per attempt and must never be journalled.

Full reads verify the SHA-256, because a truncated transfer is the one corruption
the server cannot see: it never held the bytes. Range reads do not, because a
range does not hash to the object's digest.

Upload **inside a step**. That is what puts the reference in the journal, and the
journal reference is what keeps the bytes alive.

For journals too large to ship inline, `App.journalSource(base, token)` supplies
the address for §8.6 paging. Without it a truncated attempt fails non-retryably
naming the method — the only safe alternative, because replaying a partial
journal re-executes every step the app could not see while the run still
completes.

## 7. Testing surface

```ts
const t = harness(chargeAndShip);
t.sendEvent('order.approved', true);          // BEFORE the wait
expect(await t.runToCompletion()).toBe(true);
t.assertStepExecutedOnce('charge');
```

No database, no server, no HTTP. The harness drives the **same `runPass`** the
real handler drives; one with its own replay logic would let a workflow pass here
and fail in production for reasons the test could not see.

Two fidelity rules are load-bearing:

* **Only settled steps enter the memo.** Shipping a pending row would be more
  permissive than the engine, and a workflow could pass its tests and hang the
  first time it was deployed.
* **The fake must model what the engine does, not what is convenient.** The
  driver originally recorded a timed-out wait as `completed` with no data. The
  engine records `timed_out`. The test passed against a fake that agreed with the
  bug in §4 above, and the battery caught what it could not.

`assertStepExecutedOnce` is the assertion worth reaching for: at-least-once
execution is the contract, so "ran once" is a property of memoisation working
rather than something a handler can arrange for itself.

## 8. Package layout

```
spec/typescript/            @stepd/protocol      no I/O, no timers, one dependency
sdk/typescript/
  packages/sdk-core/        @stepd/sdk-core      no I/O, no HTTP, no clock
  packages/sdk/             @stepd/sdk           + /node, + /testing
  apps/conformance/         the §12.2 battery app
```

The boundaries are checked, not documented: `boundaries.test.ts` in each package
asserts no `node:` import, no `fetch`, no timer, no clock, and no second
dependency. An `import 'node:fs'` in the protocol package would work perfectly,
pass every other test, and quietly make it unusable on the edge runtimes an SDK
is supposed to reach.

`cd sdk/typescript && pnpm test` needs nothing running and takes about a second,
which is why those suites are in CI's tier 1 rather than tier 2.

## 9. Verification status

| Claim | Evidence |
|---|---|
| Agrees with `stepd-proto` on the hash and the signature | 20 vectors in `spec/fixtures/protocol.json`, asserted by both bindings |
| The replay machinery behaves | 76 tests in `@stepd/sdk-core` |
| The serving layer behaves | 48 tests in `@stepd/sdk` |
| The whole thing satisfies the protocol | `stepd conformance`: 28/28, **level 2**, in CI on every push |

What that does **not** establish is in
[ADR-025](adr/025-typescript-sdk-divergences.md) under *What we accept*: this SDK
was written by reading the Rust one, so the two can share a misreading of the
specification. Level 2 shows the §12.2 contract is implementable twice against
one server, not that `spec/PROTOCOL.md` is sufficient alone
([#6](https://github.com/swiftugandan/stepd/issues/6)).

## 10. Open questions

1. **Framework adapters beyond Node.** `createHandler` is web-standard, so
   Express, Fastify, Hono and Next.js need thin wrappers that do not exist yet.
   The Node one is about forty lines and the conformance app uses it, which is
   the only reason it is known to work.
2. **Strict mode.** `ctx.idSequence()` exposes the ids claimed this pass, and
   §6.1 rule 5 suggests comparing it against the previous attempt to catch
   accidental non-determinism. Nothing does.
3. **Publishing.** Every package is `private: true`. Releasing means deciding on
   versioning across two languages and replacing the `link:` dependency on
   `spec/typescript` with a real version, and is a separate decision.
4. **Streaming blob reads.** §8.3.3 says an SDK *should* offer one; this reads
   whole ranges into memory, which is fine to 100 MiB and not beyond.
