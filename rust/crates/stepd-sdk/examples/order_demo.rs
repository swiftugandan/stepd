//! A worked demo: a real SDK app on a real socket, driven by a real server.
//!
//! Point it at a running `stepd dev` and it registers an app, ingests an
//! order event, waits for the run to reach its `wait_event`, approves it
//! through the operator command, and reports what the run produced and how
//! many times each step body actually executed.
//!
//!   cargo run -p stepd-sdk --example order_demo -- <base-url> <token>

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use stepd_sdk::prelude::*;
use uuid::Uuid;

/// Executions per (run, step) — the property the whole system exists to give.
static EXECUTIONS: OnceLock<Mutex<HashMap<(Uuid, &'static str), usize>>> = OnceLock::new();

fn record(run: Uuid, step: &'static str) {
    *EXECUTIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry((run, step))
        .or_insert(0) += 1;
}

fn executions(run: Uuid, step: &'static str) -> usize {
    EXECUTIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .get(&(run, step))
        .copied()
        .unwrap_or(0)
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Receipt {
    tx: String,
    carrier: String,
    approved_by: String,
}

/// A step, a parallel pair, a wait, another step.
async fn order_fulfilment(ctx: &Ctx) -> StepResult<Receipt> {
    let run_id = ctx.run().id;

    let tx: String = ctx
        .step("charge", || async move {
            record(run_id, "charge");
            println!("      [app] charge executing");
            Ok("ch_1".to_string())
        })
        .await?;

    let invoice = ctx.step::<String, _, _>("fetch-invoice", || async move {
        record(run_id, "fetch-invoice");
        Ok("inv-9".into())
    });
    let risk = ctx.step::<i32, _, _>("score-risk", || async move {
        record(run_id, "score-risk");
        Ok(17)
    });
    let (_invoice, _risk) = ctx.join((invoice, risk)).await?;
    println!("      [app] parallel pair committed");

    let approval: Option<serde_json::Value> = ctx.wait_event("approval", "order.approved").await?;
    println!("      [app] wait resolved");

    let carrier: String = ctx
        .step("ship", || async move {
            record(run_id, "ship");
            println!("      [app] ship executing");
            Ok("dhl".to_string())
        })
        .await?;

    Ok(Receipt {
        tx,
        carrier,
        approved_by: approval
            .and_then(|v| v["by"].as_str().map(str::to_string))
            .unwrap_or_else(|| "nobody".into()),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let base = args.next().unwrap_or_else(|| "http://127.0.0.1:8080".into());
    let token = args.next().expect("usage: order_demo <base-url> <token>");
    // `stepd dev` signs with this unless STEPD_SIGNING_KEY overrides it.
    let signing_key = std::env::var("STEPD_SIGNING_KEY")
        .unwrap_or_else(|_| "dev-signing-key".into())
        .into_bytes();

    // The app needs a port before the server can be told about it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let app_url = format!("http://{}", listener.local_addr()?);
    println!("  [1] app listening on {app_url}");

    let app = App::new("billing-demo", app_url.clone())
        .signing_key(signing_key)
        .function(
            Function::new("order-fulfilment")
                .on_event("order.created")
                .key("'order:' + string(event.data.order_id)")
                .run(order_fulfilment),
        );
    let manifest = app.manifest();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.router()).await;
    });

    let http = reqwest::Client::new();
    // A fresh order per invocation: the key is the single-writer identity, and
    // reusing it would queue this run behind the last one still waiting.
    let order_id = Uuid::new_v4().as_u128() as u32 % 100_000;
    let want_key = format!("order:{order_id}");

    // Register through the real API, exactly as an SDK does at start-up.
    let res = http
        .put(format!("{base}/v1/apps"))
        .bearer_auth(&token)
        .json(&manifest)
        .send()
        .await?;
    println!("  [2] registered app        -> HTTP {}", res.status());
    if !res.status().is_success() {
        println!("      {}", res.text().await?);
        return Err("registration failed".into());
    }

    // Ingest the trigger event.
    let res: serde_json::Value = http
        .post(format!("{base}/v1/events"))
        .bearer_auth(&token)
        .json(&serde_json::json!([{
            "specversion": "1.0",
            "source": "/shop",
            "type": "order.created",
            "data": { "order_id": order_id },
        }]))
        .send()
        .await?
        .json()
        .await?;
    println!(
        "  [3] ingested order.created -> accepted={} runs_started={}",
        res["accepted"], res["runs_started"]
    );

    // Find the run this trigger started, by the key its CEL expression produced.
    let run = find_run(&http, &base, &token, &want_key, 40)
        .await
        .ok_or("no run was started")?;
    let run_id = run["id"].as_str().unwrap().to_string();
    println!("  [4] run {run_id}  key={}", run["key"]);

    // Wait until the handler has parked on its wait_event.
    let waiting = poll_run(
        &http,
        &base,
        &token,
        &run_id,
        |r| r["status"] == "waiting" || r["status"] == "completed",
        60,
    )
    .await;
    println!(
        "  [5] run reached           -> status={}",
        waiting
            .as_ref()
            .map(|r| r["status"].clone())
            .unwrap_or_default()
    );

    // Approve it through the operator command — the path the console uses.
    let res = http
        .post(format!("{base}/v1/runs/{run_id}/resolve-wait"))
        .bearer_auth(&token)
        .json(&serde_json::json!({
            "event_type": "order.approved",
            "data": { "by": "priya" }
        }))
        .send()
        .await?;
    println!("  [6] resolve-wait          -> HTTP {}", res.status());

    // Drive to completion.
    let done = poll_run(
        &http,
        &base,
        &token,
        &run_id,
        |r| r["status"] == "completed" || r["status"] == "failed",
        90,
    )
    .await
    .ok_or("run did not settle")?;

    println!("\n  ── result ─────────────────────────────────");
    println!("  status   {}", done["status"]);
    println!("  output   {}", done["output"]);

    let steps: serde_json::Value = http
        .get(format!("{base}/v1/runs/{run_id}/steps"))
        .bearer_auth(&token)
        .send()
        .await?
        .json()
        .await?;
    let mut ids: Vec<String> = steps["steps"]
        .as_object()
        .map(|m| {
            m.values()
                .filter_map(|s| {
                    Some(format!(
                        "{}({})",
                        s["id"].as_str()?,
                        s["status"].as_str().unwrap_or("?")
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    println!("  steps    {ids:?}");

    let uuid: Uuid = run_id.parse()?;
    println!("\n  ── executions (memoisation) ───────────────");
    for step in ["charge", "fetch-invoice", "score-risk", "ship"] {
        println!("  {step:<14} executed {} time(s)", executions(uuid, step));
    }

    let ok = done["status"] == "completed"
        && ["charge", "ship"].iter().all(|s| executions(uuid, s) == 1);
    println!(
        "\n  {}",
        if ok {
            "PASS — run completed and no step body executed twice"
        } else {
            "FAIL — see above"
        }
    );
    if !ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Poll the run list until a run carrying `key` appears.
async fn find_run(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    key: &str,
    ticks: u32,
) -> Option<serde_json::Value> {
    for _ in 0..ticks {
        let body: serde_json::Value = http
            .get(format!("{base}/v1/runs"))
            .bearer_auth(token)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        if let Some(run) = body["items"]
            .as_array()
            .and_then(|a| a.iter().find(|r| r["key"] == key))
        {
            return Some(run.clone());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

/// Poll one run's detail until `pred` holds, or the budget runs out.
async fn poll_run(
    http: &reqwest::Client,
    base: &str,
    token: &str,
    run_id: &str,
    pred: impl Fn(&serde_json::Value) -> bool,
    ticks: u32,
) -> Option<serde_json::Value> {
    for _ in 0..ticks {
        let run: serde_json::Value = http
            .get(format!("{base}/v1/runs/{run_id}"))
            .bearer_auth(token)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        if pred(&run) {
            return Some(run);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}
