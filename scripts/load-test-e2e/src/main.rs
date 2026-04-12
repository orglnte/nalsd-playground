use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Parser)]
struct Args {
    /// Base URL of the bench service
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,

    /// Total requests per second (split 50/50 between /db and /store)
    #[arg(long, default_value = "1000")]
    rps: u64,

    /// Duration in seconds
    #[arg(long, default_value = "300")]
    duration: u64,

    /// Concurrent workers per endpoint
    #[arg(long, default_value = "8")]
    concurrency: usize,
}

struct Stats {
    ok: AtomicU64,
    err: AtomicU64,
    hist: Mutex<Histogram<u64>>,
    first_err: Mutex<Option<String>>,
}

impl Stats {
    fn new() -> Self {
        Self {
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            hist: Mutex::new(Histogram::new_with_max(30_000_000, 3).unwrap()),
            first_err: Mutex::new(None),
        }
    }

    async fn record_ok(&self, latency: Duration) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        let us = latency.as_micros() as u64;
        let mut h = self.hist.lock().await;
        let _ = h.record(us);
    }

    async fn record_err(&self, latency: Duration, msg: String) {
        self.err.fetch_add(1, Ordering::Relaxed);
        let us = latency.as_micros() as u64;
        let mut h = self.hist.lock().await;
        let _ = h.record(us);
        let mut first = self.first_err.lock().await;
        if first.is_none() {
            *first = Some(msg);
        }
    }

    async fn summary(&self) -> String {
        let ok = self.ok.load(Ordering::Relaxed);
        let err = self.err.load(Ordering::Relaxed);
        let h = self.hist.lock().await;
        let p50 = h.value_at_quantile(0.50) as f64 / 1000.0;
        let p99 = h.value_at_quantile(0.99) as f64 / 1000.0;
        let mean = h.mean() / 1000.0;
        format!(
            "ok={ok:>7}  err={err:>4}  p50={p50:>7.1}ms  p99={p99:>7.1}ms  mean={mean:>7.1}ms"
        )
    }
}

async fn run_endpoint(
    label: &str,
    stats: Arc<Stats>,
    http: reqwest::Client,
    base_url: String,
    rps_per_worker: u64,
    deadline: Instant,
) {
    let interval = Duration::from_nanos(1_000_000_000 / rps_per_worker.max(1));
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut i: u64 = 0;

    let post_url = base_url.clone();
    let get_url = base_url;

    while Instant::now() < deadline {
        ticker.tick().await;
        let t0 = Instant::now();

        let result = if i % 2 == 0 {
            // Write
            http.post(&post_url).send().await
        } else {
            // Read
            http.get(&get_url).send().await
        };

        match result {
            Ok(resp) if resp.status().is_success() => {
                // Consume body to free connection
                let _ = resp.bytes().await;
                stats.record_ok(t0.elapsed()).await;
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                stats
                    .record_err(
                        t0.elapsed(),
                        format!("{label}: HTTP {status}: {body}"),
                    )
                    .await;
            }
            Err(e) => {
                stats
                    .record_err(t0.elapsed(), format!("{label}: {e}"))
                    .await;
            }
        }

        i += 1;
    }
}

fn container_mem_mb(name: &str) -> Option<f64> {
    let out = std::process::Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{.MemUsage}}", name])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    let usage = s.trim().split('/').next()?.trim();
    if usage.contains("GiB") {
        return usage.replace("GiB", "").trim().parse::<f64>().ok().map(|v| v * 1024.0);
    }
    if usage.contains("MiB") {
        return usage.replace("MiB", "").trim().parse::<f64>().ok();
    }
    if usage.contains("KiB") {
        return usage.replace("KiB", "").trim().parse::<f64>().ok().map(|v| v / 1024.0);
    }
    None
}

fn snapshot() -> Vec<(String, f64)> {
    let out = std::process::Command::new("docker")
        .args(["ps", "--filter", "name=nalsd-measure-", "--format", "{{.Names}}"])
        .output()
        .expect("docker ps failed");
    let names = String::from_utf8_lossy(&out.stdout);
    let mut snap = Vec::new();
    for name in names.trim().lines() {
        if let Some(mem) = container_mem_mb(name) {
            snap.push((name.to_string(), mem));
        }
    }
    snap.sort_by(|a, b| a.0.cmp(&b.0));
    snap
}

fn get_process_rss(name: &str) -> Option<f64> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", name])
        .output()
        .ok()?;
    let pid_str = String::from_utf8_lossy(&out.stdout);
    let pid = pid_str.trim().lines().next()?.trim();

    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", pid])
        .output()
        .ok()?;
    let rss_kb: f64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    Some(rss_kb / 1024.0) // KB to MB
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let duration = Duration::from_secs(args.duration);
    let half_rps = args.rps / 2;
    let rps_per_worker = half_rps / args.concurrency as u64;

    println!("============================================================");
    println!(
        "nalsd — E2E Load Test ({} rps total × {}s)",
        args.rps, args.duration,
    );
    println!(
        "  /db:    {} rps ({} workers)",
        half_rps, args.concurrency
    );
    println!(
        "  /store: {} rps ({} workers)",
        half_rps, args.concurrency
    );
    println!("============================================================");

    // Baseline
    println!("\n[1/4] Baseline RSS ...");
    let before = snapshot();
    if let Some(svc) = get_process_rss("bench_service") {
        println!("  {:<42} {:>6.1} MB", "bench_service", svc);
    }
    for (name, mem) in &before {
        println!("  {name:<42} {mem:>6.1} MB");
    }

    let deadline = Instant::now() + duration;
    let db_stats = Arc::new(Stats::new());
    let store_stats = Arc::new(Stats::new());

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency * 2)
        .build()?;

    println!(
        "\n[2/4] Running load: {} rps × {}s ...",
        args.rps, args.duration
    );

    let mut handles = Vec::new();

    // /db workers
    for _ in 0..args.concurrency {
        let stats = db_stats.clone();
        let http = http.clone();
        let url = format!("{}/db", args.url);
        handles.push(tokio::spawn(async move {
            run_endpoint("db", stats, http, url, rps_per_worker, deadline).await;
        }));
    }

    // /store workers
    for _ in 0..args.concurrency {
        let stats = store_stats.clone();
        let http = http.clone();
        let url = format!("{}/store", args.url);
        handles.push(tokio::spawn(async move {
            run_endpoint("store", stats, http, url, rps_per_worker, deadline).await;
        }));
    }

    // Progress
    let progress = tokio::spawn(async move {
        let start = Instant::now();
        let total = duration.as_secs();
        loop {
            time::sleep(Duration::from_secs(5)).await;
            let elapsed = start.elapsed().as_secs();
            if elapsed >= total {
                break;
            }
            eprint!("\r  [{elapsed}/{total}s]");
        }
        eprintln!("\r  [{total}/{total}s]");
    });

    for h in handles {
        let _ = h.await;
    }
    let _ = progress.await;

    // Snapshot at end of load
    println!("\n[3/4] RSS at end of load ...");
    let after = snapshot();

    println!(
        "\n  {:<42} {:>12} {:>12} {:>12}",
        "Component", "Before", "After", "Delta"
    );
    println!("  {}", "─".repeat(78));

    if let (Some(svc_before), Some(svc_after)) = (
        get_process_rss("bench_service"),
        get_process_rss("bench_service"),
    ) {
        let d = svc_after - svc_before;
        let sign = if d >= 0.0 { "+" } else { "" };
        println!(
            "  {:<42} {:>9.1} MB {:>9.1} MB {sign}{d:>8.1} MB",
            "bench_service (Python)", svc_before, svc_after
        );
    }

    let mut total_before = 0.0_f64;
    let mut total_after = 0.0_f64;
    for (name, mem_after) in &after {
        let mem_before = before
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, m)| *m)
            .unwrap_or(0.0);
        let delta = mem_after - mem_before;
        let sign = if delta >= 0.0 { "+" } else { "" };
        println!(
            "  {name:<42} {mem_before:>9.1} MB {mem_after:>9.1} MB {sign}{delta:>8.1} MB"
        );
        total_before += mem_before;
        total_after += *mem_after;
    }
    let td = total_after - total_before;
    let sign = if td >= 0.0 { "+" } else { "" };
    println!("  {}", "─".repeat(78));
    println!(
        "  {:<42} {:>9.1} MB {:>9.1} MB {sign}{td:>8.1} MB",
        "TOTAL (containers)", total_before, total_after
    );

    // Settle
    println!("\n[4/4] Settling (5s) ...");
    time::sleep(Duration::from_secs(5)).await;

    // Stats
    println!("\n{}", "─".repeat(60));
    println!("  Endpoint results ({} rps target × {}s):", args.rps, args.duration);
    for (label, st) in [("/db", &db_stats), ("/store", &store_stats)] {
        println!("  {:<12}  {}", label, st.summary().await);
        if let Some(e) = st.first_err.lock().await.as_ref() {
            println!("  {:<12}  first error: {}", "", e);
        }
    }
    println!();

    Ok(())
}
