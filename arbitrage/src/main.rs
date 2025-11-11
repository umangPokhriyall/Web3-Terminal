// src/main.rs
use anyhow::Result;
use dashmap::DashMap;
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, time::sleep};

/// ========== CONFIG ============
const CHANNEL_PATTERN: &str = "orderbook:*";
const MPSC_CAPACITY: usize = 32_000; // channel capacity for backpressure
const BATCH_MAX: usize = 256; // max messages processed per worker batch
                              // const WORKERS: usize = 4; // adjust to #logical cores (num_cpus)
const METRICS_INTERVAL_SECS: u64 = 1;

// ========== DOMAIN TYPES ==========
#[derive(Debug, Clone, Deserialize, Serialize)]
struct OrderBookTop {
    exchange: String,
    symbol: String,
    bid: f64,
    ask: f64,
    bid_qty: f64,
    ask_qty: f64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ArbitrageSignal {
    symbol: String,
    buy_exchange: String,
    sell_exchange: String,
    buy_price: f64,
    sell_price: f64,
    spread_pct: f64,
    net_pct: f64,
    volume: f64,
    timestamp: u64,
    status: String,
}

#[derive(Clone)]
struct ExchangeBook {
    books: HashMap<String, OrderBookTop>, // exchange -> top
    max_bid: Option<(String, f64, f64)>,  // (exchange, price, qty)
    min_ask: Option<(String, f64, f64)>,
}

impl ExchangeBook {
    fn new() -> Self {
        Self {
            books: HashMap::new(),
            max_bid: None,
            min_ask: None,
        }
    }

    fn update_exchange(&mut self, tob: OrderBookTop) {
        // keep ownership (avoid cloning outside)
        self.books.insert(tob.exchange.clone(), tob);
        self.recompute_minmax();
    }

    fn recompute_minmax(&mut self) {
        let mut max_bid: Option<(String, f64, f64)> = None;
        let mut min_ask: Option<(String, f64, f64)> = None;

        for (ex, b) in &self.books {
            if b.bid > 0.0 {
                match &max_bid {
                    None => max_bid = Some((ex.clone(), b.bid, b.bid_qty)),
                    Some((_, price, _)) if b.bid > *price => {
                        max_bid = Some((ex.clone(), b.bid, b.bid_qty))
                    }
                    _ => {}
                }
            }

            if b.ask > 0.0 {
                match &min_ask {
                    None => min_ask = Some((ex.clone(), b.ask, b.ask_qty)),
                    Some((_, price, _)) if b.ask < *price => {
                        min_ask = Some((ex.clone(), b.ask, b.ask_qty))
                    }
                    _ => {}
                }
            }
        }

        self.max_bid = max_bid;
        self.min_ask = min_ask;
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ========== MAIN ==========
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting Optimized Arbitrage Engine...");
    let start = Instant::now();

    // Redis client (Arc so workers can create their own connections)
    let redis_uri = "redis://127.0.0.1/";
    let client = Arc::new(redis::Client::open(redis_uri)?);

    // Channel: ingestion -> workers
    let (tx, rx) = mpsc::channel::<String>(MPSC_CAPACITY);

    // DashMap shared state (lock-free concurrent map)
    let books: Arc<DashMap<String, ExchangeBook>> = Arc::new(DashMap::new());

    // fee table (Arc for cheap clone into workers)
    #[derive(Clone)]
    struct Fee {
        maker: f64,
        taker: f64,
    }
    let fees_map: Arc<HashMap<String, Fee>> = Arc::new(HashMap::from([
        (
            "binance".to_string(),
            Fee {
                maker: 0.00075,
                taker: 0.00075,
            },
        ),
        (
            "bybit".to_string(),
            Fee {
                maker: 0.0010,
                taker: 0.0010,
            },
        ),
        (
            "kraken".to_string(),
            Fee {
                maker: 0.0025,
                taker: 0.0040,
            },
        ),
        (
            "hyperliquid".to_string(),
            Fee {
                maker: 0.0004,
                taker: 0.0007,
            },
        ),
    ]));

    // Symbol normalization: use a static HashMap to avoid per-tick expensive ops.
    // Expand/modify this as needed (populate from config on startup).
    let norm_map: Arc<HashMap<String, String>> = Arc::new({
        let mut m = HashMap::new();
        m.insert("BTC/USD".to_string(), "BTCUSDT".to_string());
        m.insert("ETH/USD".to_string(), "ETHUSDT".to_string());
        m.insert("BTCUSDC".to_string(), "BTCUSDT".to_string()); // example
        m.insert("BTCUSDC".to_string(), "BTCUSDT".to_string());
        // hyperliquid coins (raw coin -> pair)
        m.insert("BTC".to_string(), "BTCUSDC".to_string());
        m.insert("ETH".to_string(), "ETHUSDC".to_string());
        m
    });

    // Metrics counters
    let recv_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let proc_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let parse_total_nanos = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Spawn metrics printer
    {
        let recv = recv_count.clone();
        let proc = proc_count.clone();
        let parse_nanos = parse_total_nanos.clone();
        tokio::spawn(async move {
            let mut uptime_secs = 0u64;
            let mut last_recv = 0u64;
            let mut last_proc = 0u64;
            loop {
                sleep(Duration::from_secs(METRICS_INTERVAL_SECS)).await;
                uptime_secs += METRICS_INTERVAL_SECS;
                let curr_recv = recv.load(std::sync::atomic::Ordering::Relaxed);
                let curr_proc = proc.load(std::sync::atomic::Ordering::Relaxed);
                let diff_recv = curr_recv.saturating_sub(last_recv);
                let diff_proc = curr_proc.saturating_sub(last_proc);
                last_recv = curr_recv;
                last_proc = curr_proc;

                let avg_parse_latency_ms = {
                    let p = parse_nanos.load(std::sync::atomic::Ordering::Relaxed);
                    if curr_proc == 0 {
                        0.0
                    } else {
                        (p as f64) / (curr_proc as f64) / 1_000_000.0
                    }
                };

                println!(
                    "📊 [METRICS] +{} msgs recv/sec | +{} processed/sec | total recv={} proc={} | uptime={}s | avg_parse_ms={:.6}",
                    diff_recv / (METRICS_INTERVAL_SECS as u64),
                    diff_proc / (METRICS_INTERVAL_SECS as u64),
                    curr_recv,
                    curr_proc,
                    uptime_secs,
                    avg_parse_latency_ms
                );
            }
        });
    }

    // Spawn ingestion task: reads redis pubsub and pushes raw messages into channel
    {
        let client = client.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            // create a pubsub connection + subscribe
            if let Ok(mut conn) = client.get_async_connection().await {
                let mut pubsub = conn.into_pubsub();
                if let Err(e) = pubsub.psubscribe(CHANNEL_PATTERN).await {
                    eprintln!("❌ Failed to subscribe to {} : {:?}", CHANNEL_PATTERN, e);
                    return;
                }
                println!("✅ Ingestion: subscribed to {}", CHANNEL_PATTERN);

                let mut stream = pubsub.on_message();
                while let Some(msg) = stream.next().await {
                    // Pull payload string and push to channel. If channel is full -> drop to apply backpressure
                    match msg.get_payload::<String>() {
                        Ok(payload) => {
                            recv_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // try_send is intentionally used to avoid awaiting on backpressure in ingestion (keeps pubsub loop responsive)
                            if let Err(e) = tx.try_send(payload) {
                                // Channel full – we drop the message (or you can track dropped count). This is a policy decision.
                                // In HFT you would apply backpressure earlier or increase MPSC_CAPACITY.
                                eprintln!("⚠️ ingestion dropped message: {:?}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("⚠️ ingestion couldn't get payload: {:?}", e);
                        }
                    }
                }
            } else {
                eprintln!("❌ ingestion failed to create redis connection");
            }
        });
    }

    let num_cores = num_cpus::get();
    println!(
        "🧠 Detected {} CPU cores — spawning that many workers",
        num_cores
    );

    // Spawn worker pool
    let rx = Arc::new(tokio::sync::Mutex::new(rx)); // worker(s) will lock to pull batches
    for worker_id in 0..num_cores {
        let rx = rx.clone();
        let books = books.clone();
        let fees = fees_map.clone();
        let client = client.clone();
        // let recv_ctr = recv_count.clone();
        let proc_ctr = proc_count.clone();
        let parse_nanos = parse_total_nanos.clone();
        let norm_map = norm_map.clone();

        tokio::spawn(async move {
            // create per-worker publishing connection
            let mut pub_conn = match client.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "❌ worker{} failed to create redis conn: {:?}",
                        worker_id, e
                    );
                    return;
                }
            };

            // prealloc batch buffer once per worker to avoid repeated allocs
            let mut batch: Vec<String> = Vec::with_capacity(BATCH_MAX);

            loop {
                // Build batch: one blocking recv, then drain up to BATCH_MAX immediate messages
                batch.clear();
                // lock the receiver for pulling a small batch atomically (keeps fairness)
                {
                    let mut guard = rx.lock().await;
                    // get the first element (await)
                    match guard.recv().await {
                        Some(msg) => batch.push(msg),
                        None => {
                            // channel closed
                            break;
                        }
                    }
                    // try to drain more without awaiting to form a batch
                    for _ in 0..(BATCH_MAX - 1) {
                        match guard.try_recv() {
                            Ok(m) => batch.push(m),
                            Err(_) => break,
                        }
                    }
                } // release lock on rx

                // parse & process batch
                for raw in &batch {
                    let parse_start = Instant::now();
                    // Try fast simd-json parsing into serde Value-ish. If simd-json not available or fails, fall back to serde_json.
                    // Note: simd-json expects a mutable string; we clone into a temporary mutable buffer here.
                    let parsed_result: Option<OrderBookTop> = {
                        // Attempt simd-json path
                        #[cfg(feature = "fast-simd")]
                        {
                            // simd-json usage: parse in-place requires &mut String
                            let mut tmp = raw.clone();
                            match simd_json::to_borrowed_value(&mut tmp) {
                                Ok(bv) => {
                                    // safe, light-weight extraction
                                    let maybe = extract_orderbook_from_borrowed_value(&bv);
                                    maybe
                                }
                                Err(_) => None,
                            }
                        }
                        #[cfg(not(feature = "fast-simd"))]
                        {
                            // fallback path – use serde_json
                            match serde_json::from_str::<OrderBookTop>(raw) {
                                Ok(ob) => Some(ob),
                                Err(_) => None,
                            }
                        }
                    };

                    // If simd-json feature is enabled but extraction returned None, try serde_json fallback
                    let ob = match parsed_result {
                        Some(ob) => ob,
                        None => match serde_json::from_str::<OrderBookTop>(raw) {
                            Ok(ob) => ob,
                            Err(e) => {
                                eprintln!("⚠️ parse error (both fast & serde failed): {}", e);
                                continue;
                            }
                        },
                    };

                    let parse_elapsed = parse_start.elapsed();
                    parse_nanos.fetch_add(
                        parse_elapsed.as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    // normalize symbol using precomputed map (cheap map lookup)
                    let symbol_key = {
                        if let Some(mapped) = norm_map.get(&ob.symbol) {
                            mapped.clone()
                        } else {
                            // fallback normalization: uppercase + remove slash (cheapish)
                            ob.symbol.to_uppercase().replace("/", "")
                        }
                    };

                    // Update central DashMap
                    // We avoid clones by moving `ob` into the book map.
                    // DashMap API: use entry then modify in place (here we use
                    // insert/get_mut pattern that worked earlier in your codebase).
                    {
                        // Insert or update the ExchangeBook for this symbol
                        // (We create a new ExchangeBook if missing.)
                        // Note: This does allocate string keys if new symbol seen.
                        books
                            .entry(symbol_key.clone())
                            .or_insert_with(ExchangeBook::new);
                        // Now mutate the entry (get_mut returns a RefMut)
                        if let Some(mut guard) = books.get_mut(&symbol_key) {
                            // move OrderBookTop into the map value
                            guard.update_exchange(ob);
                        }
                    }

                    proc_ctr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    // After update, check bests and compute arbitrage
                    if let Some(mut guard) = books.get_mut(&symbol_key) {
                        if let (
                            Some((sell_ex, sell_price, sell_qty)),
                            Some((buy_ex, buy_price, buy_qty)),
                        ) = (&guard.max_bid, &guard.min_ask)
                        {
                            // ensure different exchanges
                            if sell_ex != buy_ex {
                                // compute gross spread and fees (use taker fees)
                                let gross_spread = (sell_price - buy_price) / buy_price;
                                let buy_fee = fees.get(buy_ex).map(|f| f.taker).unwrap_or(0.001);
                                let sell_fee = fees.get(sell_ex).map(|f| f.taker).unwrap_or(0.001);
                                let net_spread = gross_spread - (buy_fee + sell_fee);

                                // volume available to arbitrage
                                let volume = buy_qty.min(*sell_qty);

                                if net_spread > 0.0005 {
                                    let now = current_millis();
                                    let arb = ArbitrageSignal {
                                        symbol: symbol_key.clone(),
                                        buy_exchange: buy_ex.clone(),
                                        sell_exchange: sell_ex.clone(),
                                        buy_price: *buy_price,
                                        sell_price: *sell_price,
                                        spread_pct: gross_spread * 100.0,
                                        net_pct: net_spread * 100.0,
                                        volume,
                                        timestamp: now,
                                        status: "open".into(),
                                    };
                                    // publish signal (serde JSON for visibility; later replace with bincode)
                                    match serde_json::to_vec(&arb) {
                                        Ok(bytes) => {
                                            let _ = pub_conn
                                                .publish::<_, _, i64>("signals:arbitrage", bytes)
                                                .await;
                                        }
                                        Err(e) => {
                                            eprintln!("⚠️ arb serialization failed: {:?}", e);
                                        }
                                    }
                                }
                            }
                        }
                    } // guard drop
                } // for each in batch
            } // loop
        });
    }

    // Wait forever (or until ctrl+c). The main thread could instead await a signal.
    println!("Engine started in {:.3}s", start.elapsed().as_secs_f64());
    // keep main alive
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

/// Extract minimal OrderBookTop manually from simd-json BorrowedValue.
/// Keep this small and inlined so simd-json path is cheap.
///
/// NOTE: this function only compiled when `fast-simd` feature is enabled.
/// We include this helper for completeness; if you don't enable simd-json,
/// the fallback serde_json path is used instead.
#[cfg(feature = "fast-simd")]
fn extract_orderbook_from_borrowed_value(bv: &simd_json::BorrowedValue) -> Option<OrderBookTop> {
    use simd_json::BorrowedValue as BV;
    // expected top-level object has fields exactly matching OrderBookTop
    if let BV::Object(map) = bv {
        let get_str = |key: &str| -> Option<&str> { map.get(key)?.as_str() };
        let get_f64_from_str_field =
            |key: &str| -> Option<f64> { map.get(key)?.as_str()?.parse::<f64>().ok() };
        let exchange = get_str("exchange")?.to_string();
        let symbol = get_str("symbol")?.to_string();
        let bid = get_f64_from_str_field("bid")?;
        let ask = get_f64_from_str_field("ask")?;
        let bid_qty = get_f64_from_str_field("bid_qty")?;
        let ask_qty = get_f64_from_str_field("ask_qty")?;
        let timestamp = map
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .unwrap_or(current_millis());
        Some(OrderBookTop {
            exchange,
            symbol,
            bid,
            ask,
            bid_qty,
            ask_qty,
            timestamp,
        })
    } else {
        None
    }
}
