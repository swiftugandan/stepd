# stepd — the TypeScript SDK

What a workflow author imports. No engine, no database, no server.

```
packages/sdk-core/    the replay machinery — no I/O, no HTTP
packages/sdk/         the builders, the manifest, the request handler
apps/conformance/     the §12.2 battery app — never published, never a production route
```

**`stepd conformance` reports this app CONFORMANT AT LEVEL 1**, with all twelve
declared cases passing. The twelve level-2 suites are not declared, so the runner
reports them as unknowns that bar level 2 rather than as failures — the blob
client, the §8.6 paging and the level-2 handlers are not written yet. The root
[`README.md`](../../README.md) says what is finished and what is not.

The SDK's surface is `(Request) => Promise<Response>`, so the same handler runs
under `node:http`, Bun, Deno, a Cloudflare Worker or a Next.js route. Framework
adapters are not written yet; `apps/conformance/src/server.ts` has the `node:http`
bridge in about forty lines, which is roughly what one will be.

## Why this is a separate workspace

The same reason [`sdk/rust`](../rust) is: so that "a workflow author's process
contains no engine" fails a build rather than appearing in a diagram
([ADR-024](../../docs/adr/024-language-trees.md)). The whole dependency graph is:

```
  @stepd/sdk-core
      └── @stepd/protocol        ../../spec/typescript
```

`@stepd/protocol` is a path dependency on `spec/typescript`, not a package here.
It is the published wire contract, shared with the engine and every other SDK;
owning it from one side would make it that side's type definitions.

Build the protocol package first — `sdk-core` resolves it through `dist`:

```bash
(cd ../../spec/typescript && pnpm install && pnpm build)
pnpm install && pnpm test
```

## The one rule

**`ctx.step(id, fn)` claims its occurrence when it is *called*, not when the
returned value is awaited.**

Everything else follows. Claiming at `await` would tie occurrence to scheduler
order rather than declaration order, so a completed step's hash would differ
between attempts: the server holds a result the handler never asks for, the step
re-executes, the payment is taken twice — and **nothing errors**
([ADR-012](../../docs/adr/012-eager-occurrence-claiming.md)).

Because the claim is already done, `Promise.all` over steps is safe.
`ctx.parallel` adds a repeated-id check and one envelope instead of one round
trip per member.

`test/claiming.test.ts` demonstrates this rather than asserting it: one test
awaits the same steps in opposite orders and shows the hashes are unchanged, and
a second implements lazy claiming as a deliberate control and asserts that it
*does* break. If the control ever passes, the first test is proving nothing.

## What replaces Rust's `!Send`

The Rust SDK makes `Ctx` `!Send`, so spawning a task that claims a step fails to
compile. TypeScript has no equivalent, so the runtime guards here are not
belt-and-braces — they are the only line of defence:

| Guard | What it catches |
|---|---|
| Pass token / sealing | A claim after the pass ended: a timer, a floating promise, a `Ctx` that outlived its attempt |
| No claiming inside a step body | A nested `ctx.step`, whose hash would depend on whether its parent was replayed from the memo. The Rust SDK permits this; refusing is deliberate |
| No repeated id in one parallel group | A fan-out loop with no discriminator |
| Swallowed-halt detection | `try { await ctx.step(..) } catch {}`, and `Promise.allSettled` over steps |

That last one matters more here than in Rust. A step that has recorded its result
throws to stop the pass so the server can commit it; `catch {}` and `allSettled`
both absorb that, and the run would be committed as complete with the rest of the
workflow never run. `allSettled` in particular is a reasonable thing to reach for
rather than a mistake anyone would flag in review — it was found by writing a
test that expected it to work.

## Two deliberate differences from the Rust SDK

**Step results are projected through JSON on first execution too.** Rust returns
the original value the first time and the JSON projection on replay, so
`ctx.step('t', () => new Date())` hands back a `Date` once and an ISO string ever
after — code that works until the first retry, which is exactly when nobody is
watching. One round trip per step buys identical behaviour on attempt one and
attempt forty.

**A step body may not create steps.** See the table above.

## Running the tests

No database, no server, no `STEPD_TEST_DATABASE_URL`:

```bash
pnpm test        # 106 tests: 76 in sdk-core, 30 in sdk
pnpm typecheck
pnpm build       # ESM, CJS and .d.ts
```

## Running the battery against it

```bash
(cd ../../spec/typescript && pnpm build) && pnpm build
(cd apps/conformance && PORT=9944 node --experimental-strip-types src/main.ts &)

cd ../../engine/rust
cargo run -p stepd-cli -- \
  --database-url postgres://postgres@127.0.0.1:5433/stepd_conf_ts \
  conformance \
  --app http://127.0.0.1:9944 \
  --app-configure-url http://127.0.0.1:9944/_conformance/configure
```

`--app-configure-url` is how an app the runner did not start learns the server's
address and a token. It is not needed at level 1 and is passed anyway, because
the level-2 suites that need it are the next thing to land.

CI runs exactly this, in `tier 2 · integration`.
