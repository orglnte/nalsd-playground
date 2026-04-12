use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::time;

// 1000 bytes payload
const PAYLOAD: &str = concat!(
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 60
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 120
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 180
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 240
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 300
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 360
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 420
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 480
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 540
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 600
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 660
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 720
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 780
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 840
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 900
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", // 960
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",                     // 1000
);

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value = "15432")]
    port: u16,
    #[arg(long, default_value = "platform")]
    user: String,
    #[arg(long, default_value = "platform-local-password")]
    password: String,
    #[arg(long, default_value = "appdb")]
    database: String,
    /// Target writes per second (total across all workers)
    #[arg(long, default_value = "500")]
    wps: u64,
    /// Duration in seconds
    #[arg(long, default_value = "180")]
    duration: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let duration = Duration::from_secs(args.duration);
    let connstr = format!(
        "host={} port={} user={} password={} dbname={}",
        args.host, args.port, args.user, args.password, args.database
    );

    println!("bench-postgres: {} wps × {}s", args.wps, args.duration);
    println!("payload size: {} bytes", PAYLOAD.len());

    // Connect and create table
    let (setup_client, setup_conn) =
        tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
    tokio::spawn(async move { setup_conn.await.ok(); });
    setup_client
        .execute("DROP TABLE IF EXISTS bench_pg", &[])
        .await?;
    setup_client
        .execute(
            "CREATE TABLE bench_pg (
                id SERIAL PRIMARY KEY,
                payload TEXT NOT NULL
            )",
            &[],
        )
        .await?;
    drop(setup_client);

    let total_writes = Arc::new(AtomicU64::new(0));
    let total_errors = Arc::new(AtomicU64::new(0));
    let first_write_ms = Arc::new(AtomicU64::new(0));
    let last_write_ms = Arc::new(AtomicU64::new(0));

    let deadline = Instant::now() + duration;

    // Progress logger
    let writes_ref = total_writes.clone();
    let errors_ref = total_errors.clone();
    let progress = tokio::spawn(async move {
        let start = Instant::now();
        let total_s = duration.as_secs();
        loop {
            time::sleep(Duration::from_secs(5)).await;
            let elapsed = start.elapsed().as_secs();
            let w = writes_ref.load(Ordering::Relaxed);
            let e = errors_ref.load(Ordering::Relaxed);
            let rate = if elapsed > 0 { w / elapsed } else { 0 };
            eprintln!(
                "[{:>4}s/{:>4}s] writes={:>7}  errors={:>4}  rate={:>5} w/s",
                elapsed, total_s, w, e, rate
            );
            if elapsed >= total_s {
                break;
            }
        }
    });

    // Single connection, single task
    let (client, conn) =
        tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
    tokio::spawn(async move { conn.await.ok(); });

    let stmt = client
        .prepare("INSERT INTO bench_pg (payload) VALUES ($1)")
        .await?;

    let interval = Duration::from_nanos(1_000_000_000 / args.wps.max(1));
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    while Instant::now() < deadline {
        ticker.tick().await;
        match client.execute(&stmt, &[&PAYLOAD]).await {
            Ok(_) => {
                let ts = now_ms();
                total_writes.fetch_add(1, Ordering::Relaxed);
                first_write_ms.compare_exchange(0, ts, Ordering::Relaxed, Ordering::Relaxed).ok();
                last_write_ms.store(ts, Ordering::Relaxed);
            }
            Err(e) => {
                let prev = total_errors.fetch_add(1, Ordering::Relaxed);
                if prev == 0 {
                    eprintln!("first error: {e}");
                }
            }
        }
    }
    progress.await?;

    // Validation
    let (client, conn) =
        tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
    tokio::spawn(async move { conn.await.ok(); });
    let row = client
        .query_one("SELECT COUNT(*) FROM bench_pg", &[])
        .await?;
    let db_count: i64 = row.get(0);

    let counted_writes = total_writes.load(Ordering::Relaxed);
    let counted_errors = total_errors.load(Ordering::Relaxed);
    let first_ms = first_write_ms.load(Ordering::Relaxed);
    let last_ms = last_write_ms.load(Ordering::Relaxed);
    let wall_s = if last_ms > first_ms {
        (last_ms - first_ms) as f64 / 1000.0
    } else {
        1.0
    };
    let actual_wps = db_count as f64 / wall_s;

    // Disk usage
    let disk_row = client
        .query_one(
            "SELECT pg_total_relation_size('bench_pg')",
            &[],
        )
        .await?;
    let disk_bytes: i64 = disk_row.get(0);

    println!("\n========== RESULTS ==========");
    println!("counted writes:   {}", counted_writes);
    println!("counted errors:   {}", counted_errors);
    println!("db row count:     {}", db_count);
    println!("match:            {}", if counted_writes == db_count as u64 { "YES" } else { "NO" });
    println!("first write:      {} ms", first_ms);
    println!("last write:       {} ms", last_ms);
    println!("wall time:        {:.1} s", wall_s);
    println!("actual rate:      {:.1} writes/s", actual_wps);
    println!("disk usage:       {:.1} MB", disk_bytes as f64 / 1024.0 / 1024.0);
    println!("=============================");

    Ok(())
}
