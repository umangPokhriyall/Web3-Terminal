// bybit.rs (rewritten, optimized, compatible with your collectors)
use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use crate::metrics::ThroughputMetrics;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::Value;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

// fast binary serializer
use bincode;

/// Collector for Bybit (v5 public spot)
#[derive(Clone)]
pub struct BybitCollector {
    pub symbols: Vec<String>,
    pub redis_client: redis::Client,
    /// topics per WS connection (tune when testing)
    pub max_topics_per_conn: usize,
}

impl Default for BybitCollector {
    fn default() -> Self {
        Self {
            symbols: vec![],
            redis_client: redis::Client::open("redis://127.0.0.1/").unwrap(),
            max_topics_per_conn: 9,
        }
    }
}

// batching and connectivity
const BATCH_FLUSH: usize = 100;
const MAXLEN: usize = 10_000;
const PING_INTERVAL_SECS: u64 = 20;
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_MAX_SECS: u64 = 60;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct BybitTickerData {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: Option<String>,
    #[serde(rename = "highPrice24h")]
    high_24h: Option<String>,
    #[serde(rename = "lowPrice24h")]
    low_24h: Option<String>,
    #[serde(rename = "volume24h")]
    volume_24h: Option<String>,
}

#[async_trait::async_trait]
impl ExchangeClient for BybitCollector {
    fn name(&self) -> &'static str {
        "bybit"
    }

    async fn connect_price_stream(&mut self) -> Result<()> {
        // we'll handle tickers, orderbook.1 (TOB), and publicTrade topics here,
        // splitting them into multiple connections if needed.
        let symbols = self.symbols.clone();
        let redis_client = self.redis_client.clone();
        let max_per_conn = std::cmp::max(self.max_topics_per_conn, 1);

        // build topics
        let mut topics = Vec::with_capacity(symbols.len() * 3);
        for s in &symbols {
            topics.push(format!("tickers.{}", s));
            topics.push(format!("orderbook.1.{}", s)); // top-of-book
            topics.push(format!("publicTrade.{}", s));
        }

        for (i, chunk) in topics.chunks(max_per_conn).enumerate() {
            let chunk_topics = chunk.to_vec();
            let rclient = redis_client.clone();

            // metrics per connection
            let metrics = ThroughputMetrics::new();
            let label = format!("conn{}", i);
            let metrics_clone = metrics.clone();
            tokio::spawn(async move {
                metrics_clone.start_logger("bybit", &label).await;
            });

            // spawn connection task with stagger & reconnect
            tokio::spawn(async move {
                sleep(Duration::from_millis(200 * (i as u64))).await;
                let mut backoff = Duration::from_millis(RECONNECT_BASE_MS);
                loop {
                    match run_bybit_conn(chunk_topics.clone(), &rclient, metrics.clone()).await {
                        Ok(_) => {
                            backoff = Duration::from_millis(RECONNECT_BASE_MS);
                        }
                        Err(e) => {
                            eprintln!(
                                "❌ [Bybit] conn ({}topics) failed: {:?}. reconnecting {:?}",
                                chunk_topics.len(),
                                e,
                                backoff
                            );
                            let jitter = rand::random::<u64>() % 1000;
                            sleep(backoff + Duration::from_millis(jitter)).await;
                            backoff =
                                std::cmp::min(backoff * 2, Duration::from_secs(RECONNECT_MAX_SECS));
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn connect_orderbook_stream(&mut self) -> Result<()> {
        // topics already created in connect_price_stream
        Ok(())
    }
    async fn connect_trades_stream(&mut self) -> Result<()> {
        Ok(())
    }

    async fn get_snapshot(&self, _symbol: &str) -> Result<OrderBookSnapshot> {
        Ok(OrderBookSnapshot {
            lastUpdateId: 0,
            bids: vec![],
            asks: vec![],
        })
    }
}

/// Run single Bybit WS connection (topics provided)
async fn run_bybit_conn(
    topics: Vec<String>,
    redis_client: &redis::Client,
    metrics: ThroughputMetrics,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let url = Url::parse("wss://stream.bybit.com/v5/public/spot")?;
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("User-Agent", "web3-terminal/collector".parse().unwrap());

    let (ws_stream, _) = connect_async(req).await?;
    println!("✅ [Bybit] handshake OK ({} topics)", topics.len());

    let (write, mut read) = ws_stream.split();
    let write = Arc::new(Mutex::new(write));

    // Subscribe once
    let subscribe = serde_json::json!({ "op": "subscribe", "args": topics });
    {
        let mut w = write.lock().await;
        w.send(Message::Text(subscribe.to_string())).await?;
    }

    // ping loop
    let ping_writer = write.clone();
    let ping_handle = tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(PING_INTERVAL_SECS)).await;
            let ping = serde_json::json!({ "op": "ping" });
            let mut w = ping_writer.lock().await;
            if let Err(e) = w.send(Message::Text(ping.to_string())).await {
                eprintln!("❌ [Bybit] ping failed: {:?}", e);
                break;
            }
        }
    });

    // Redis batching buffers
    let mut redis_conn = redis_client.get_async_connection().await?;
    let mut batch_prices: Vec<Vec<u8>> = Vec::with_capacity(BATCH_FLUSH);
    let mut batch_orderbooks: Vec<Vec<u8>> = Vec::with_capacity(BATCH_FLUSH);
    let mut batch_trades: Vec<Vec<u8>> = Vec::with_capacity(BATCH_FLUSH);

    // optional small in-memory TOB cache per connection
    let mut best: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }

        metrics.incr_recv();

        // parse fast (simd-json expects mutable bytes)
        let mut bytes = msg.to_text()?.as_bytes().to_vec();
        let mut v: Value = match simd_json::serde::from_slice(&mut bytes) {
            Ok(val) => val,
            Err(_) => continue,
        };

        // handle possible ret_code errors (v5)
        if let Some(rc) = v.get("ret_code").and_then(|x| x.as_i64()) {
            if rc != 0 {
                ping_handle.abort();
                return Err(anyhow::anyhow!("bybit ret_code {}: {:?}", rc, v));
            }
        }

        // subscription ack
        if let Some(op) = v.get("op").and_then(|x| x.as_str()) {
            if op == "subscribe" {
                // skip ack
                continue;
            }
        }

        if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
            if topic.starts_with("tickers.") {
                if let Err(e) = handle_ticker_simd(&mut v, &mut batch_prices, &metrics).await {
                    eprintln!("❌ [Bybit ticker handler]: {:?}", e);
                }
            } else if topic.starts_with("orderbook.1.") {
                if let Err(e) =
                    handle_orderbook_simd(&mut v, &mut best, &mut batch_orderbooks, &metrics).await
                {
                    eprintln!("❌ [Bybit orderbook handler]: {:?}", e);
                }
            } else if topic.starts_with("publicTrade.") {
                if let Err(e) = handle_trade_simd(&mut v, &mut batch_trades, &metrics).await {
                    eprintln!("❌ [Bybit trade handler]: {:?}", e);
                }
            }
        }

        // flush batches when full (pipelined)
        if batch_prices.len() >= BATCH_FLUSH {
            let mut pipe = redis::pipe();
            for payload in batch_prices.drain(..) {
                pipe.cmd("XADD")
                    .arg("prices:bybit")
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(MAXLEN)
                    .arg("*")
                    .arg("data")
                    .arg(payload);
            }
            let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
        }
        if batch_orderbooks.len() >= BATCH_FLUSH {
            let mut pipe = redis::pipe();
            for payload in batch_orderbooks.drain(..) {
                pipe.cmd("XADD")
                    .arg("orderbook:bybit")
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(MAXLEN)
                    .arg("*")
                    .arg("data")
                    .arg(payload);
            }
            let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
        }
        if batch_trades.len() >= BATCH_FLUSH {
            let mut pipe = redis::pipe();
            for payload in batch_trades.drain(..) {
                pipe.cmd("XADD")
                    .arg("trades:bybit")
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(MAXLEN)
                    .arg("*")
                    .arg("data")
                    .arg(payload);
            }
            let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
        }
    }

    // final flush
    if !batch_prices.is_empty() {
        let mut pipe = redis::pipe();
        for payload in batch_prices.drain(..) {
            pipe.cmd("XADD")
                .arg("prices:bybit")
                .arg("MAXLEN")
                .arg("~")
                .arg(MAXLEN)
                .arg("*")
                .arg("data")
                .arg(payload);
        }
        let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
    }
    if !batch_orderbooks.is_empty() {
        let mut pipe = redis::pipe();
        for payload in batch_orderbooks.drain(..) {
            pipe.cmd("XADD")
                .arg("orderbook:bybit")
                .arg("MAXLEN")
                .arg("~")
                .arg(MAXLEN)
                .arg("*")
                .arg("data")
                .arg(payload);
        }
        let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
    }
    if !batch_trades.is_empty() {
        let mut pipe = redis::pipe();
        for payload in batch_trades.drain(..) {
            pipe.cmd("XADD")
                .arg("trades:bybit")
                .arg("MAXLEN")
                .arg("~")
                .arg(MAXLEN)
                .arg("*")
                .arg("data")
                .arg(payload);
        }
        let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
    }

    ping_handle.abort();
    Err(anyhow::anyhow!("bybit ws read ended"))
}

// ---------------- Handlers (SIMD parsed Value -> bincode -> batch) ----------------

async fn handle_ticker_simd(
    v: &mut Value,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    if let Some(data) = v.get("data") {
        // data might be an object or array
        if data.is_array() {
            for item in data.as_array().unwrap().iter() {
                if let Ok(td) = serde_json::from_value::<BybitTickerData>(item.clone()) {
                    publish_ticker(td, v.get("ts").and_then(|x| x.as_u64()), batch, metrics)
                        .await?;
                }
            }
        } else if data.is_object() {
            if let Ok(td) = serde_json::from_value::<BybitTickerData>(data.clone()) {
                publish_ticker(td, v.get("ts").and_then(|x| x.as_u64()), batch, metrics).await?;
            }
        }
    }
    Ok(())
}

async fn publish_ticker(
    td: BybitTickerData,
    ts_opt: Option<u64>,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    let nd = NormalizedData {
        exchange: "bybit".into(),
        symbol: td.symbol.clone(),
        price: td
            .last_price
            .as_deref()
            .unwrap_or("0")
            .parse()
            .unwrap_or(0.0),
        volume: td
            .volume_24h
            .as_deref()
            .unwrap_or("0")
            .parse()
            .unwrap_or(0.0),
        high: td.high_24h.as_deref().unwrap_or("0").parse().unwrap_or(0.0),
        low: td.low_24h.as_deref().unwrap_or("0").parse().unwrap_or(0.0),
        timestamp: ts_opt.unwrap_or_else(current_millis),
    };

    let payload: Vec<u8> = bincode::serialize(&nd)?;
    batch.push(payload);
    metrics.incr_pub();
    Ok(())
}

/// handle top-of-book orderbook updates
async fn handle_orderbook_simd(
    v: &mut Value,
    best: &mut HashMap<String, (f64, f64, f64, f64)>,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    // expect v.data.b / v.data.a as arrays of arrays: [["price","size"], ...]
    if let Some(data) = v.get("data") {
        // data may be object (single symbol) or array
        // For the v5 public API, 'data' is often an object with 'b' and 'a'
        let bids_outer = data
            .get("b")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        let asks_outer = data
            .get("a")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();

        if bids_outer.is_empty() || asks_outer.is_empty() {
            return Ok(());
        }

        // extract first inner array elements safely
        let (bid, bid_qty) = {
            let first = bids_outer.get(0).and_then(|v| v.as_array());
            if let Some(inner) = first {
                let px = inner.get(0).and_then(|x| x.as_str()).unwrap_or("0");
                let sz = inner.get(1).and_then(|x| x.as_str()).unwrap_or("0");
                (
                    px.parse::<f64>().unwrap_or(0.0),
                    sz.parse::<f64>().unwrap_or(0.0),
                )
            } else {
                (0.0, 0.0)
            }
        };

        let (ask, ask_qty) = {
            let first = asks_outer.get(0).and_then(|v| v.as_array());
            if let Some(inner) = first {
                let px = inner.get(0).and_then(|x| x.as_str()).unwrap_or("0");
                let sz = inner.get(1).and_then(|x| x.as_str()).unwrap_or("0");
                (
                    px.parse::<f64>().unwrap_or(0.0),
                    sz.parse::<f64>().unwrap_or(0.0),
                )
            } else {
                (0.0, 0.0)
            }
        };

        if bid <= 0.0 || ask <= 0.0 {
            return Ok(());
        }

        // symbol from topic: topic = "orderbook.1.SYMBOL"
        let symbol = v
            .get("topic")
            .and_then(|t| t.as_str())
            .map(|s| s.trim_start_matches("orderbook.1.").to_string())
            .unwrap_or_else(|| "".into());
        if symbol.is_empty() {
            return Ok(());
        }

        best.insert(symbol.clone(), (bid, ask, bid_qty, ask_qty));

        let ob = OrderBookTop {
            exchange: "bybit".into(),
            symbol: symbol.clone(),
            bid,
            ask,
            bid_qty,
            ask_qty,
            timestamp: v
                .get("ts")
                .and_then(|t| t.as_u64())
                .unwrap_or_else(current_millis),
        };

        let payload: Vec<u8> = bincode::serialize(&ob)?;
        batch.push(payload);
        metrics.incr_pub();
    }
    Ok(())
}

async fn handle_trade_simd(
    v: &mut Value,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    // data is often an array of trade objects
    if let Some(arr) = v.get("data").and_then(|x| x.as_array()) {
        for item in arr.iter() {
            let symbol = item
                .get("s")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if symbol.is_empty() {
                continue;
            }
            let price = item
                .get("p")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let qty = item
                .get("q")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let side = if item.get("m").and_then(|x| x.as_bool()).unwrap_or(false) {
                "sell".to_string()
            } else {
                "buy".to_string()
            };
            let timestamp = item
                .get("T")
                .and_then(|x| x.as_u64())
                .unwrap_or_else(current_millis);

            if price <= 0.0 || qty <= 0.0 {
                continue;
            }

            let ev = TradeEvent {
                exchange: "bybit".into(),
                symbol,
                price,
                qty,
                side,
                timestamp,
            };

            let payload: Vec<u8> = bincode::serialize(&ev)?;
            batch.push(payload);
            metrics.incr_pub();
        }
    }
    Ok(())
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
