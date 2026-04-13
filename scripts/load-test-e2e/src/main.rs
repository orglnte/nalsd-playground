//! E2E load test — photo app workload mix.
//!
//! Simulates realistic photo app traffic against bench_service.py:
//!   Upload  (POST /photos)         — 10%  — postgres + S3 write
//!   View    (GET  /photos/{id})    — 50%  — postgres read + S3 read
//!   List    (GET  /photos?page=N)  — 30%  — postgres paginated query
//!   Search  (GET  /photos/search)  — 10%  — postgres full-text search

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    /// Total requests per second
    #[arg(long, default_value = "1000")]
    rps: u64,
    /// Duration in seconds
    #[arg(long, default_value = "3600")]
    duration: u64,
    /// Concurrent workers
    #[arg(long, default_value = "8")]
    concurrency: usize,
}

const SEARCH_WORDS: &[&str] = &[
    "sunset", "beach", "mountain", "city", "forest", "river", "snow",
    "garden", "portrait", "street", "night", "morning", "autumn", "spring",
];

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
            "ok={ok:>8}  err={err:>5}  p50={p50:>7.1}ms  p99={p99:>7.1}ms  mean={mean:>7.1}ms"
        )
    }
}

async fn do_request(
    http: &reqwest::Client,
    base: &str,
    op: &str,
    max_id: &AtomicU64,
    stats: &Stats,
) {
    let t0 = Instant::now();
    let mid = max_id.load(Ordering::Relaxed).max(1);

    let result = match op {
        "upload" => http.post(&format!("{base}/photos")).send().await,
        "view" => {
            let id = fastrand::u64(1..=mid);
            http.get(&format!("{base}/photos/{id}")).send().await
        }
        "list" => {
            let page = fastrand::u32(0..20);
            http.get(&format!("{base}/photos?page={page}")).send().await
        }
        "search" => {
            let word = SEARCH_WORDS[fastrand::usize(..SEARCH_WORDS.len())];
            http.get(&format!("{base}/photos/search?q={word}")).send().await
        }
        _ => unreachable!(),
    };

    match result {
        Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 404 => {
            let _ = resp.bytes().await;
            stats.record_ok(t0.elapsed()).await;

            // Update max_id from upload responses
            if op == "upload" {
                max_id.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            stats.record_err(t0.elapsed(), format!("{op}: HTTP {status}: {}", &body[..body.len().min(200)])).await;
        }
        Err(e) => {
            stats.record_err(t0.elapsed(), format!("{op}: {e}")).await;
        }
    }
}

/// Pick an operation based on the traffic mix:
///   upload=10%, view=50%, list=30%, search=10%
fn pick_op(i: u64) -> &'static str {
    match i % 100 {
        0..10 => "upload",
        10..60 => "view",
        60..90 => "list",
        _ => "search",
    }
}

fn container_stats() -> Vec<(String, String, String)> {
    let out = std::process::Command::new("docker")
        .args(["stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"])
        .output()
        .ok();
    let Some(out) = out else { return vec![] };
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().lines().map(|line| {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 3 {
            (parts[0].to_string(), parts[1].to_string(), parts[2].to_string())
        } else {
            (line.to_string(), "?".into(), "?".into())
        }
    }).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let duration = Duration::from_secs(args.duration);
    let rps_per_worker = args.rps / args.concurrency as u64;

    println!("============================================================");
    println!(
        "Photo App Load Test — {} rps × {}s ({} workers)",
        args.rps, args.duration, args.concurrency,
    );
    println!("  upload  10%  (POST /photos)        — postgres + S3 write");
    println!("  view    50%  (GET  /photos/{{id}})   — postgres + S3 read");
    println!("  list    30%  (GET  /photos?page=N)  — postgres query");
    println!("  search  10%  (GET  /photos/search)  — postgres FTS");
    println!("============================================================");

    println!("\n[1/4] Baseline ...");
    for (name, cpu, mem) in container_stats() {
        println!("  {name:<42} CPU={cpu:<8} MEM={mem}");
    }

    let deadline = Instant::now() + duration;
    let upload_stats = Arc::new(Stats::new());
    let view_stats = Arc::new(Stats::new());
    let list_stats = Arc::new(Stats::new());
    let search_stats = Arc::new(Stats::new());
    let max_id = Arc::new(AtomicU64::new(0));

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency * 4)
        .build()?;

    // Seed: upload 100 photos so view/list/search have data
    println!("\n[2/4] Seeding 100 photos ...");
    for _ in 0..100 {
        let resp = http.post(&format!("{}/photos", args.url)).send().await?;
        let _ = resp.bytes().await;
        max_id.fetch_add(1, Ordering::Relaxed);
    }
    println!("  seeded {} photos", max_id.load(Ordering::Relaxed));

    println!(
        "\n[3/4] Running load: {} rps × {}s ...",
        args.rps, args.duration,
    );

    let mut handles = Vec::new();

    for w in 0..args.concurrency {
        let http = http.clone();
        let base = args.url.clone();
        let upload_s = upload_stats.clone();
        let view_s = view_stats.clone();
        let list_s = list_stats.clone();
        let search_s = search_stats.clone();
        let mid = max_id.clone();

        handles.push(tokio::spawn(async move {
            let interval = Duration::from_nanos(1_000_000_000 / rps_per_worker.max(1));
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            let mut i: u64 = w as u64 * 37; // offset so workers don't all pick same ops

            while Instant::now() < deadline {
                ticker.tick().await;
                let op = pick_op(i);
                let stats = match op {
                    "upload" => &*upload_s,
                    "view" => &*view_s,
                    "list" => &*list_s,
                    "search" => &*search_s,
                    _ => unreachable!(),
                };
                do_request(&http, &base, op, &mid, stats).await;
                i += 1;
            }
        }));
    }

    // Progress with periodic container stats
    let progress = tokio::spawn(async move {
        let start = Instant::now();
        let total = duration.as_secs();
        loop {
            time::sleep(Duration::from_secs(30)).await;
            let elapsed = start.elapsed().as_secs();
            if elapsed >= total { break; }
            eprint!("\r  [{elapsed}/{total}s]");
            for (name, cpu, mem) in container_stats() {
                eprint!("  {name}: CPU={cpu} MEM={mem}");
            }
        }
        eprintln!();
    });

    for h in handles {
        let _ = h.await;
    }
    let _ = progress.await;

    // End stats
    println!("\n[4/4] Container stats at end of load ...");
    for (name, cpu, mem) in container_stats() {
        println!("  {name:<42} CPU={cpu:<8} MEM={mem}");
    }

    println!("\n{}", "─".repeat(70));
    println!("  Endpoint results ({} rps target × {}s):", args.rps, args.duration);
    for (label, st) in [
        ("upload", &upload_stats),
        ("view", &view_stats),
        ("list", &list_stats),
        ("search", &search_stats),
    ] {
        println!("  {label:<12}  {}", st.summary().await);
        if let Some(e) = st.first_err.lock().await.as_ref() {
            println!("  {label:<12}  first error: {}", &e[..e.len().min(200)]);
        }
    }
    println!();

    Ok(())
}
