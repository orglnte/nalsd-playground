use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::time;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value = "19000")]
    port: u16,
    #[arg(long, default_value = "platform")]
    access_key: String,
    #[arg(long, default_value = "platform-local-password")]
    secret_key: String,
    #[arg(long, default_value = "bench")]
    bucket: String,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    /// Target writes per second (total across all workers, ignored if --count is set)
    #[arg(long, default_value = "500")]
    wps: u64,
    /// Duration in seconds (ignored if --count is set)
    #[arg(long, default_value = "180")]
    duration: u64,
    /// Fixed number of objects to write (max rate, no ticker). Overrides --wps/--duration.
    #[arg(long)]
    count: Option<u64>,
    /// Number of parallel workers (only used with --count)
    #[arg(long, default_value = "8")]
    workers: u64,
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
    // 1000 bytes, same as postgres bench
    let payload: Vec<u8> = vec![b'x'; 1000];

    println!("bench-s3: {} wps × {}s", args.wps, args.duration);
    println!("payload size: {} bytes", payload.len());
    println!(
        "endpoint: http://{}:{} bucket: {}",
        args.host, args.port, args.bucket
    );

    // Create bucket
    let region = s3::Region::Custom {
        region: args.region.clone(),
        endpoint: format!("http://{}:{}", args.host, args.port),
    };
    let creds =
        s3::creds::Credentials::new(Some(&args.access_key), Some(&args.secret_key), None, None, None)?;

    let _ = s3::Bucket::create_with_path_style(
        &args.bucket,
        region.clone(),
        creds.clone(),
        s3::BucketConfiguration::default(),
    )
    .await;

    // Fixed-count mode: N objects, max rate, no ticker
    if let Some(count) = args.count {
        let workers = args.workers.max(1);
        let per_worker = count / workers;
        let total_writes = Arc::new(AtomicU64::new(0));
        let total_errors = Arc::new(AtomicU64::new(0));

        println!("FIXED COUNT: {} objects, {} workers, max rate", count, workers);

        let t0 = Instant::now();
        let mut handles = Vec::new();
        for w in 0..workers {
            let writes = total_writes.clone();
            let errors = total_errors.clone();
            let payload = payload.clone();
            let bucket = s3::Bucket::new(&args.bucket, region.clone(), creds.clone())?
                .with_path_style();
            handles.push(tokio::spawn(async move {
                for i in 0..per_worker {
                    let key = format!("w{}-{}", w, i);
                    match bucket.put_object(&key, &payload).await {
                        Ok(resp) if resp.status_code() < 400 => {
                            writes.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(_) | Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }
        for h in handles {
            h.await?;
        }
        let elapsed = t0.elapsed().as_secs_f64();
        let w = total_writes.load(Ordering::Relaxed);
        let e = total_errors.load(Ordering::Relaxed);
        let rate = w as f64 / elapsed;

        println!("\n========== RESULTS ==========");
        println!("target count:     {}", per_worker * workers);
        println!("counted writes:   {}", w);
        println!("counted errors:   {}", e);
        println!("elapsed:          {:.2} s", elapsed);
        println!("rate:             {:.1} obj/s", rate);
        println!("=============================");
        return Ok(());
    }

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

    // Two tasks, each at half the rate, each with its own S3 client
    let half_wps = args.wps / 2;
    let mut handles = Vec::new();

    for task_id in 0..2u64 {
        let writes = total_writes.clone();
        let errors = total_errors.clone();
        let first = first_write_ms.clone();
        let last = last_write_ms.clone();
        let payload = payload.clone();
        let bucket = s3::Bucket::new(&args.bucket, region.clone(), creds.clone())?
            .with_path_style();

        handles.push(tokio::spawn(async move {
            let interval = Duration::from_nanos(1_000_000_000 / half_wps.max(1));
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            let mut seq: u64 = 0;

            while Instant::now() < deadline {
                ticker.tick().await;
                let key = format!("t{}-{}", task_id, seq);
                seq += 1;

                match bucket.put_object(&key, &payload).await {
                    Ok(resp) if resp.status_code() < 400 => {
                        let ts = now_ms();
                        writes.fetch_add(1, Ordering::Relaxed);
                        first.compare_exchange(0, ts, Ordering::Relaxed, Ordering::Relaxed).ok();
                        last.store(ts, Ordering::Relaxed);
                    }
                    Ok(resp) => {
                        let prev = errors.fetch_add(1, Ordering::Relaxed);
                        if prev == 0 {
                            eprintln!("task {task_id}: first error: HTTP {}", resp.status_code());
                        }
                    }
                    Err(e) => {
                        let prev = errors.fetch_add(1, Ordering::Relaxed);
                        if prev == 0 {
                            eprintln!("task {task_id}: first error: {e}");
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await?;
    }
    progress.await?;

    // Validation: list all objects in the bucket and count them
    let bucket = s3::Bucket::new(&args.bucket, region.clone(), creds.clone())?
        .with_path_style();

    let mut db_count: u64 = 0;
    let mut continuation_token: Option<String> = None;
    loop {
        let results = bucket
            .list(String::new(), None)
            .await?;
        for list in &results {
            db_count += list.contents.len() as u64;
            if list.next_continuation_token.is_some() {
                continuation_token = list.next_continuation_token.clone();
            } else {
                continuation_token = None;
            }
        }
        if continuation_token.is_none() {
            break;
        }
    }

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

    println!("\n========== RESULTS ==========");
    println!("counted writes:   {}", counted_writes);
    println!("counted errors:   {}", counted_errors);
    println!("s3 object count:  {}", db_count);
    println!("match:            {}", if counted_writes == db_count { "YES" } else { "NO" });
    println!("first write:      {} ms", first_ms);
    println!("last write:       {} ms", last_ms);
    println!("wall time:        {:.1} s", wall_s);
    println!("actual rate:      {:.1} writes/s", actual_wps);
    println!("data written:     {:.1} MB", (db_count * payload.len() as u64) as f64 / 1024.0 / 1024.0);
    println!("=============================");

    Ok(())
}
