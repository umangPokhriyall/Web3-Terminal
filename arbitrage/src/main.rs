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
use tokio::{sync::Mutex, time::sleep};

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
    books: HashMap<String, OrderBookTop>, // exchange → tob
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

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Starting Arbitrage Engine (Baseline Prototype)...");
    let start_time = Instant::now();

    let redis_uri = "redis://127.0.0.1/";
    let client = redis::Client::open(redis_uri)?;
    let mut pub_conn = client.get_async_connection().await?;
    let mut sub_conn = client.get_async_connection().await?.into_pubsub();

    sub_conn.psubscribe("orderbook:*").await?;
    println!("✅ Subscribed to orderbook:*");

    let books: Arc<DashMap<String, ExchangeBook>> = Arc::new(DashMap::new());

    // --- Fee table (maker/taker as decimal) ---
    #[derive(Clone)]
    struct Fee {
        maker: f64,
        taker: f64,
    }

    let fees = Arc::new(HashMap::from([
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

    // === METRICS ===
    let recv_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let proc_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Metrics printer
    {
        let recv = recv_count.clone();
        let proc = proc_count.clone();
        tokio::spawn(async move {
            let mut last_recv = 0;
            let mut last_proc = 0;
            let mut sec = 0;
            loop {
                sleep(Duration::from_secs(1)).await;
                sec += 1;
                let curr_recv = recv.load(std::sync::atomic::Ordering::Relaxed);
                let curr_proc = proc.load(std::sync::atomic::Ordering::Relaxed);
                let diff_recv = curr_recv - last_recv;
                let diff_proc = curr_proc - last_proc;
                println!(
                    "📊 [METRICS] +{} msgs recv/sec | +{} processed/sec | total recv={} proc={} | uptime={:.1}s",
                    diff_recv, diff_proc, curr_recv, curr_proc, sec as f64
                );
                last_recv = curr_recv;
                last_proc = curr_proc;
            }
        });
    }

    let mut stream = sub_conn.on_message();
    while let Some(msg) = stream.next().await {
        recv_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let payload: String = msg.get_payload()?;

        let parse_start = Instant::now();
        if let Ok(v) = serde_json::from_str::<OrderBookTop>(&payload) {
            proc_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let latency_ms = parse_start.elapsed().as_micros() as f64 / 1000.0;

            let symbol_key = normalize_symbol(&v.symbol);
            let mut entry = books
                .entry(symbol_key.clone())
                .or_insert_with(ExchangeBook::new);
            entry.update_exchange(v);

            if let (Some((sell_ex, sell_price, sell_qty)), Some((buy_ex, buy_price, buy_qty))) =
                (&entry.max_bid, &entry.min_ask)
            {
                if sell_ex == buy_ex {
                    continue;
                }

                let gross_spread = (sell_price - buy_price) / buy_price;
                let buy_fee = fees.get(buy_ex).map(|f| f.taker).unwrap_or(0.001);
                let sell_fee = fees.get(sell_ex).map(|f| f.taker).unwrap_or(0.001);
                let net_spread = gross_spread - (buy_fee + sell_fee);
                let volume = buy_qty.min(*sell_qty);
                let now = current_millis();

                if net_spread > 0.0005 {
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

                    let payload = serde_json::to_string(&arb)?;
                    let _: () = pub_conn.publish("signals:arbitrage", payload).await?;
                }
            }

            if (recv_count.load(std::sync::atomic::Ordering::Relaxed) % 1000) == 0 {
                println!(
                    "🧩 [DEBUG] Avg parse latency: {:.3} ms | total recv: {}",
                    latency_ms,
                    recv_count.load(std::sync::atomic::Ordering::Relaxed)
                );
            }
        }
    }

    println!(
        "🏁 Engine stopped after {:.2}s",
        start_time.elapsed().as_secs_f64()
    );
    Ok(())
}

fn normalize_symbol(s: &str) -> String {
    s.to_uppercase().replace("/", "")
}
