use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use tokio::sync::Mutex;
use tokio::time;

const HOST: &str = "127.0.0.1";
const PG_PORT: u16 = 15432;
const REDIS_PORT: u16 = 16379;
const MINIO_PORT: u16 = 19000;

const PG_USER: &str = "platform";
const PG_PASS: &str = "platform-local-password";
const PG_DB: &str = "appdb";
const MINIO_USER: &str = "platform";
const MINIO_PASS: &str = "platform-local-password";
const GARAGE_PORT: u16 = 13900;
const GARAGE_KEY: &str = "GK000000000000000000000000";
const GARAGE_SECRET: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Parser)]
struct Args {
    /// Target requests per second per backend
    #[arg(long, default_value = "1000")]
    rps: u64,

    /// Duration in seconds
    #[arg(long, default_value = "300")]
    duration: u64,

    /// Number of concurrent connections per backend
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

    async fn record_err(&self, latency: Duration, msg: String) {
        self.err.fetch_add(1, Ordering::Relaxed);
        let mut first = self.first_err.lock().await;
        if first.is_none() {
            *first = Some(msg);
        }
        drop(first);
        let us = latency.as_micros() as u64;
        let mut h = self.hist.lock().await;
        let _ = h.record(us);
    }

    async fn record(&self, latency: Duration, error: bool) {
        if error {
            self.err.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ok.fetch_add(1, Ordering::Relaxed);
            let us = latency.as_micros() as u64;
            let mut h = self.hist.lock().await;
            let _ = h.record(us);
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

/// Token-bucket style rate limiter using tokio interval.
/// Each worker gets rps/concurrency share of the total rate.
async fn run_at_rate<F, Fut>(
    name: &str,
    stats: Arc<Stats>,
    rps_per_worker: u64,
    deadline: Instant,
    mut op: F,
) where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
{
    let interval = Duration::from_nanos(1_000_000_000 / rps_per_worker.max(1));
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut i: u64 = 0;

    while Instant::now() < deadline {
        ticker.tick().await;
        let t0 = Instant::now();
        match op(i).await {
            Ok(()) => stats.record(t0.elapsed(), false).await,
            Err(e) => stats.record_err(t0.elapsed(), format!("{name}: {e}")).await,
        }
        i += 1;
    }
}

// ── Postgres ────────────────────────────────────────────────────────

async fn pg_worker(
    stats: Arc<Stats>,
    rps_per_worker: u64,
    deadline: Instant,
    worker_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let connstr = format!(
        "host={HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}"
    );
    let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pg connection error: {e}");
        }
    });

    // Ensure table exists (only worker 0)
    if worker_id == 0 {
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS loadtest (
                    id SERIAL PRIMARY KEY,
                    payload TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now()
                )",
                &[],
            )
            .await?;
    }
    // Small delay so table is created before other workers hit it
    time::sleep(Duration::from_millis(100)).await;

    let insert = client
        .prepare("INSERT INTO loadtest (payload) VALUES ($1) RETURNING id")
        .await?;
    let select = client
        .prepare("SELECT id, payload FROM loadtest WHERE id = $1")
        .await?;

    run_at_rate("postgres", stats, rps_per_worker, deadline, |i| {
        let insert = &insert;
        let select = &select;
        let client = &client;
        async move {
            if i % 2 == 0 {
                // ~300 bytes payload to match other backends
                let payload = format!("row-{i:0>290}");
                client.execute(insert, &[&payload]).await?;
            } else {
                let id = (i / 2).max(1) as i32;
                client.query_opt(select, &[&id]).await?;
            }
            Ok(())
        }
    })
    .await;

    Ok(())
}

// ── Redis ───────────────────────────────────────────────────────────

async fn redis_worker(
    stats: Arc<Stats>,
    rps_per_worker: u64,
    deadline: Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = redis::Client::open(format!("redis://{HOST}:{REDIS_PORT}/"))?;
    let conn = client.get_multiplexed_async_connection().await?;

    run_at_rate("redis", stats, rps_per_worker, deadline, |i| {
        let mut conn = conn.clone();
        async move {
            if i % 2 == 0 {
                let key = format!("k:{i}");
                // ~300 bytes payload to match other backends
                let val = format!("v-{i:0>295}");
                redis::cmd("SET")
                    .arg(&key)
                    .arg(&val)
                    .query_async::<()>(&mut conn)
                    .await?;
            } else {
                let key = format!("k:{}", i - 1);
                redis::cmd("GET")
                    .arg(&key)
                    .query_async::<Option<String>>(&mut conn)
                    .await?;
            }
            Ok(())
        }
    })
    .await;

    Ok(())
}

// ── MinIO (S3-compatible via rust-s3) ────────────────────────────────

async fn minio_worker(
    stats: Arc<Stats>,
    rps_per_worker: u64,
    deadline: Instant,
    worker_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use s3::creds::Credentials;
    use s3::{Bucket, Region};

    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: format!("http://{HOST}:{MINIO_PORT}"),
    };
    let creds = Credentials::new(Some(MINIO_USER), Some(MINIO_PASS), None, None, None)?;

    let bucket = Bucket::new("loadtest", region.clone(), creds.clone())?
        .with_path_style();

    // Ensure bucket exists (only worker 0)
    if worker_id == 0 {
        let _ = Bucket::create_with_path_style(
            "loadtest", region, creds, s3::BucketConfiguration::default(),
        )
        .await;
        time::sleep(Duration::from_millis(200)).await;
    }
    time::sleep(Duration::from_millis(100)).await;

    let payload = vec![b'x'; 300];

    // Seed a few objects so GETs have something to read from the start.
    for j in 0..8u64 {
        let key = format!("seed-{worker_id}-{j}");
        let _ = bucket.put_object(&key, &payload).await;
    }

    run_at_rate("minio", stats, rps_per_worker, deadline, |i| {
        let bucket = &bucket;
        let payload = &payload;
        async move {
            if i % 2 == 0 {
                let key = format!("obj-{worker_id}-{i}");
                let code = bucket.put_object(&key, payload).await?.status_code();
                if code >= 400 {
                    return Err(format!("PUT {key}: {code}").into());
                }
            } else {
                // GET a seeded key — always exists
                let key = format!("seed-{worker_id}-{}", i % 8);
                let code = bucket.get_object(&key).await?.status_code();
                if code >= 400 {
                    return Err(format!("GET {key}: {code}").into());
                }
            }
            Ok(())
        }
    })
    .await;

    Ok(())
}

// ── Garage (S3-compatible via rust-s3) ──────────────────────────────

async fn garage_worker(
    stats: Arc<Stats>,
    rps_per_worker: u64,
    deadline: Instant,
    worker_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use s3::creds::Credentials;
    use s3::{Bucket, Region};

    let region = Region::Custom {
        region: "garage".to_string(),
        endpoint: format!("http://{HOST}:{GARAGE_PORT}"),
    };
    let creds = Credentials::new(Some(GARAGE_KEY), Some(GARAGE_SECRET), None, None, None)?;

    let bucket = Bucket::new("store-lite", region.clone(), creds.clone())?
        .with_path_style();

    let payload = vec![b'x'; 300];

    // Seed objects so GETs always hit
    for j in 0..8u64 {
        let key = format!("seed-{worker_id}-{j}");
        let _ = bucket.put_object(&key, &payload).await;
    }

    run_at_rate("garage", stats, rps_per_worker, deadline, |i| {
        let bucket = &bucket;
        let payload = &payload;
        async move {
            if i % 2 == 0 {
                let key = format!("obj-{worker_id}-{i}");
                let code = bucket.put_object(&key, payload).await?.status_code();
                if code >= 400 {
                    return Err(format!("PUT {key}: {code}").into());
                }
            } else {
                let key = format!("seed-{worker_id}-{}", i % 8);
                let code = bucket.get_object(&key).await?.status_code();
                if code >= 400 {
                    return Err(format!("GET {key}: {code}").into());
                }
            }
            Ok(())
        }
    })
    .await;

    Ok(())
}

// ── Docker stats helper ─────────────────────────────────────────────

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

// ── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let duration = Duration::from_secs(args.duration);
    let rps_per_worker = args.rps / args.concurrency as u64;

    println!("============================================================");
    println!(
        "nalsd — Rust Load Test ({} rps × {}s, {} workers/backend)",
        args.rps, args.duration, args.concurrency
    );
    println!("============================================================");

    // Baseline
    println!("\n[1/4] Baseline RSS ...");
    let before = snapshot();
    for (name, mem) in &before {
        println!("  {name:<42} {mem:>6.1} MB");
    }

    let deadline = Instant::now() + duration;

    let pg_stats = Arc::new(Stats::new());
    let rd_stats = Arc::new(Stats::new());
    let s3_stats = Arc::new(Stats::new());
    let garage_stats = Arc::new(Stats::new());

    println!(
        "\n[2/4] Running load: {} rps × {}s per backend ({} workers each) ...",
        args.rps, args.duration, args.concurrency
    );

    let mut handles = Vec::new();

    // Spawn postgres workers
    for w in 0..args.concurrency {
        let stats = pg_stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = pg_worker(stats, rps_per_worker, deadline, w).await {
                eprintln!("pg worker {w} error: {e}");
            }
        }));
    }

    // Spawn redis workers
    for _ in 0..args.concurrency {
        let stats = rd_stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = redis_worker(stats, rps_per_worker, deadline).await {
                eprintln!("redis worker error: {e}");
            }
        }));
    }

    // Spawn minio workers
    for w in 0..args.concurrency {
        let stats = s3_stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = minio_worker(stats, rps_per_worker, deadline, w).await {
                eprintln!("minio worker error: {e}");
            }
        }));
    }

    // Spawn garage workers
    for w in 0..args.concurrency {
        let stats = garage_stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = garage_worker(stats, rps_per_worker, deadline, w).await {
                eprintln!("garage worker error: {e}");
            }
        }));
    }

    // Progress reporter
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

    // Wait for all workers
    for h in handles {
        let _ = h.await;
    }
    let _ = progress.await;

    // Snapshot under load (right at the end)
    println!("\n[3/4] RSS at end of load ...");
    let after = snapshot();

    println!(
        "\n  {:<42} {:>12} {:>12} {:>12}",
        "Component", "Before", "After", "Delta"
    );
    println!("  {}", "─".repeat(78));
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
        "TOTAL", total_before, total_after
    );

    // Cool-down
    println!("\n[4/4] Settling (5s) ...");
    time::sleep(Duration::from_secs(5)).await;
    let settled = snapshot();
    println!(
        "\n  {:<42} {:>12} {:>12} {:>12}",
        "Component", "Under load", "Settled", "Delta"
    );
    println!("  {}", "─".repeat(78));
    for (name, mem_settled) in &settled {
        let mem_load = after
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, m)| *m)
            .unwrap_or(0.0);
        let delta = mem_settled - mem_load;
        let sign = if delta >= 0.0 { "+" } else { "" };
        println!(
            "  {name:<42} {mem_load:>9.1} MB {mem_settled:>9.1} MB {sign}{delta:>8.1} MB"
        );
    }

    // Print load stats
    println!("\n{}", "─".repeat(60));
    println!("  Load results ({} rps target × {}s):", args.rps, args.duration);
    for (label, st) in [("postgres", &pg_stats), ("redis", &rd_stats), ("minio", &s3_stats), ("garage", &garage_stats)] {
        println!("  {:<12}  {}", label, st.summary().await);
        if let Some(e) = st.first_err.lock().await.as_ref() {
            println!("  {:<12}  first error: {}", "", e);
        }
    }
    println!();

    Ok(())
}
