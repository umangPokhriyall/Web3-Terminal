// main.rs (Arbitrage Engine — Redis Streams edition)
// Fixes: Fee scope + proper Redis Streams parsing

use anyhow::Result;
use bincode;
use dashmap::DashMap;
use redis::{AsyncCommands, FromRedisValue, RedisResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderBookTop {
    pub exchange: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbitrageSignal {
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

// === Fee moved to top-level so worker functions can reference it ===
#[derive(Clone, Debug)]
pub struct Fee {
    pub taker: f64,
}

#[derive(Clone)]
struct ExchangeBook {
    books: HashMap<String, OrderBookTop>, // exchange -> TOB
    max_bid: Option<(String, f64, f64)>,
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

    fn update(&mut self, tob: OrderBookTop) {
        self.books.insert(tob.exchange.clone(), tob);
        self.recompute();
    }

    fn recompute(&mut self) {
        let mut best_bid: Option<(String, f64, f64)> = None;
        let mut best_ask: Option<(String, f64, f64)> = None;

        for (ex, b) in &self.books {
            if b.bid > 0.0 {
                match &best_bid {
                    None => best_bid = Some((ex.clone(), b.bid, b.bid_qty)),
                    Some((_, price, _)) if b.bid > *price => {
                        best_bid = Some((ex.clone(), b.bid, b.bid_qty))
                    }
                    _ => {}
                }
            }
            if b.ask > 0.0 {
                match &best_ask {
                    None => best_ask = Some((ex.clone(), b.ask, b.ask_qty)),
                    Some((_, price, _)) if b.ask < *price => {
                        best_ask = Some((ex.clone(), b.ask, b.ask_qty))
                    }
                    _ => {}
                }
            }
        }

        self.max_bid = best_bid;
        self.min_ask = best_ask;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting Arbitrage Engine V2 (Redis Streams)...");

    let redis_uri = "redis://127.0.0.1/";
    let client = Arc::new(redis::Client::open(redis_uri)?);

    // shared state
    let books: Arc<DashMap<String, ExchangeBook>> = Arc::new(DashMap::new());

    // fee table
    let fees: Arc<HashMap<String, Fee>> = Arc::new(HashMap::from([
        ("binance".into(), Fee { taker: 0.00075 }),
        ("bybit".into(), Fee { taker: 0.0010 }),
        ("kraken".into(), Fee { taker: 0.0040 }),
        ("hyperliquid".into(), Fee { taker: 0.0007 }),
    ]));

    // Streams produced by your collectors
    let streams = vec![
        "orderbook:binance".to_string(),
        "orderbook:bybit".to_string(),
        "orderbook:kraken".to_string(),
        "orderbook:hyperliquid".to_string(),
    ];

    // ensure consumer group exists for each stream
    for s in &streams {
        let mut conn = client.get_async_connection().await?;
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(s)
            .arg("arb")
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                println!("📌 Created consumer group for {}", s);
            }
            Err(e) => {
                // Ignore "group already exists" error
                if !e.to_string().contains("BUSYGROUP") {
                    eprintln!("❌ XGROUP CREATE error for {}: {:?}", s, e);
                } else {
                    println!("📎 Consumer group already exists for {}", s);
                }
            }
        }
    }

    // spawn workers (one per core)
    let cpus = num_cpus::get();
    println!("🧠 Spawning {} workers", cpus);

    for wid in 0..cpus {
        let client = client.clone();
        let books = books.clone();
        let fees = fees.clone();
        let streams_clone = streams.clone();
        tokio::spawn(async move {
            worker_loop(wid, client, books, fees, streams_clone).await;
        });
    }

    println!("Engine started.");
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

async fn worker_loop(
    worker_id: usize,
    client: Arc<redis::Client>,
    books: Arc<DashMap<String, ExchangeBook>>,
    fees: Arc<HashMap<String, Fee>>,
    streams: Vec<String>,
) {
    let mut conn = match client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ worker{}: redis conn error: {:?}", worker_id, e);
            return;
        }
    };

    println!("👷 worker{} ready", worker_id);

    loop {
        // Build XREADGROUP command dynamically:
        // XREADGROUP GROUP arb <consumer> BLOCK 200 COUNT 200 STREAMS <streams...> <'>'...>
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg("arb")
            .arg(format!("worker{}", worker_id));
        cmd.arg("BLOCK").arg(200);
        cmd.arg("COUNT").arg(200);
        cmd.arg("STREAMS");
        for s in &streams {
            cmd.arg(s);
        }
        // one '>' per stream
        for _ in 0..streams.len() {
            cmd.arg(">");
        }

        let reply: RedisResult<redis::Value> = cmd.query_async(&mut conn).await;
        let reply = match reply {
            Ok(r) => r,
            Err(_) => {
                // transient error / timeout, continue
                continue;
            }
        };

        // parse into StreamReadReply (convenient structured type)
        let srr: redis::streams::StreamReadReply = match redis::from_redis_value(&reply) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("worker{}: parse stream reply failed: {:?}", worker_id, e);
                continue;
            }
        };

        // iterate keys (streams)
        for key in srr.keys {
            let stream_name = key.key.clone();
            let entries = key.ids;
            if entries.is_empty() {
                continue;
            }
            println!(
                "📥 worker{} read {} entries from {}",
                worker_id,
                entries.len(),
                stream_name
            );

            for entry in entries {
                let id = entry.id;
                // entry.map is Vec<(String, redis::Value)>
                for (field, value) in entry.map {
                    if field == "data" {
                        // expect the field value to be a binary blob Vec<u8>
                        if let Ok(bin) = Vec::<u8>::from_redis_value(&value) {
                            if let Ok(ob) = bincode::deserialize::<OrderBookTop>(&bin) {
                                // process (sync-safe)
                                process_orderbook(ob, &books, &fees).await;
                            } else {
                                eprintln!(
                                    "worker{}: bincode deserialize failed for {}",
                                    worker_id, stream_name
                                );
                            }
                        } else {
                            // try string path (some collectors may have pushed text accidentally)
                            if let Ok(s) = String::from_redis_value(&value) {
                                // attempt serde_json parse
                                if let Ok(ob) = serde_json::from_str::<OrderBookTop>(&s) {
                                    process_orderbook(ob, &books, &fees).await;
                                }
                            }
                        }
                    }
                }

                // ack the message
                let _ = redis::cmd("XACK")
                    .arg(&stream_name)
                    .arg("arb")
                    .arg(id.to_string())
                    .query_async::<_, ()>(&mut conn)
                    .await;
            }
        }
    }
}

async fn process_orderbook(
    ob: OrderBookTop,
    books: &Arc<DashMap<String, ExchangeBook>>,
    fees: &Arc<HashMap<String, Fee>>,
) {
    let symbol = ob.symbol.clone();

    books
        .entry(symbol.clone())
        .or_insert_with(ExchangeBook::new);
    if let Some(mut guard) = books.get_mut(&symbol) {
        guard.update(ob);
    } else {
        return;
    }

    // read-only clone for decision
    if let Some(bk) = books.get(&symbol) {
        let (sell_ex, sell_price, sell_qty) = match &bk.max_bid {
            Some(v) => v.clone(),
            None => return,
        };
        let (buy_ex, buy_price, buy_qty) = match &bk.min_ask {
            Some(v) => v.clone(),
            None => return,
        };

        if sell_ex == buy_ex {
            return;
        }

        let gross_spread = (sell_price - buy_price) / buy_price;
        let buy_fee = fees.get(&buy_ex).map(|f| f.taker).unwrap_or(0.001);
        let sell_fee = fees.get(&sell_ex).map(|f| f.taker).unwrap_or(0.001);
        let net_spread = gross_spread - (buy_fee + sell_fee);

        let volume = buy_qty.min(sell_qty);

        println!(
            "💰 ARB {}: BUY {}@{:.8} SELL {}@{:.8} NET={:.4}%",
            symbol,
            buy_ex,
            buy_price,
            sell_ex,
            sell_price,
            net_spread * 100.0
        );

        if net_spread > 0.0005 {
            println!(
                "💰 ARB {}: BUY {}@{:.8} SELL {}@{:.8} NET={:.4}%",
                symbol,
                buy_ex,
                buy_price,
                sell_ex,
                sell_price,
                net_spread * 100.0
            );
        }
    }
}
