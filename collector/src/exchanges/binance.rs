use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use crate::metrics::ThroughputMetrics;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::Value;

use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// binary serializer
use bincode;

// Batch sizes
const BATCH_FLUSH: usize = 100;
const MAXLEN: usize = 10_000;

// Reconnect parameters
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_MAX_SECS: u64 = 60;

#[derive(Clone)]
pub struct BinanceCollector {
    pub symbols: Vec<String>,
    pub redis_client: redis::Client,

    /// topics per websocket (recommended: around 50–150)
    pub max_topics_per_conn: usize,
}

impl Default for BinanceCollector {
    fn default() -> Self {
        Self {
            symbols: vec![],
            redis_client: redis::Client::open("redis://127.0.0.1/").unwrap(),
            max_topics_per_conn: 100,
        }
    }
}

#[async_trait::async_trait]
impl ExchangeClient for BinanceCollector {
    fn name(&self) -> &'static str {
        "binance"
    }

    // --------------------- PRICE STREAM (miniTicker) ---------------------
    async fn connect_price_stream(&mut self) -> Result<()> {
        let redis = self.redis_client.clone();
        let max = self.max_topics_per_conn;

        // Build all price topics
        let mut topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@miniTicker", s.to_lowercase()))
            .collect();

        if topics.is_empty() {
            return Ok(());
        }

        // Chunk and spawn connections
        for (i, chunk) in topics.chunks(max).enumerate() {
            let chunk = chunk.to_vec();
            let redis_client = redis.clone();

            let metrics = ThroughputMetrics::new();
            let label = format!("price_conn{}", i);
            tokio::spawn({
                let m = metrics.clone();
                async move { m.start_logger("binance_price", &label).await }
            });

            tokio::spawn(run_binance_conn(
                chunk,
                "prices:binance".into(),
                redis_client,
                metrics,
                handle_price_msg,
            ));
        }

        Ok(())
    }

    // ---------------------- ORDERBOOK (bookTicker) ----------------------
    async fn connect_orderbook_stream(&mut self) -> Result<()> {
        let redis = self.redis_client.clone();
        let max = self.max_topics_per_conn;

        let mut topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();

        if topics.is_empty() {
            return Ok(());
        }

        for (i, chunk) in topics.chunks(max).enumerate() {
            let chunk = chunk.to_vec();
            let redis_client = redis.clone();

            let metrics = ThroughputMetrics::new();
            let label = format!("ob_conn{}", i);
            tokio::spawn({
                let m = metrics.clone();
                async move { m.start_logger("binance_orderbook", &label).await }
            });

            tokio::spawn(run_binance_conn(
                chunk,
                "orderbook:binance".into(),
                redis_client,
                metrics,
                handle_orderbook_msg,
            ));
        }

        Ok(())
    }

    // --------------------------- TRADES ---------------------------
    async fn connect_trades_stream(&mut self) -> Result<()> {
        let redis = self.redis_client.clone();
        let max = self.max_topics_per_conn;

        let mut topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@trade", s.to_lowercase()))
            .collect();

        if topics.is_empty() {
            return Ok(());
        }

        for (i, chunk) in topics.chunks(max).enumerate() {
            let chunk = chunk.to_vec();
            let redis_client = redis.clone();

            let metrics = ThroughputMetrics::new();
            let label = format!("trade_conn{}", i);
            tokio::spawn({
                let m = metrics.clone();
                async move { m.start_logger("binance_trades", &label).await }
            });

            tokio::spawn(run_binance_conn(
                chunk,
                "trades:binance".into(),
                redis_client,
                metrics,
                handle_trade_msg,
            ));
        }

        Ok(())
    }

    async fn get_snapshot(&self, symbol: &str) -> Result<OrderBookSnapshot> {
        let url = format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=100",
            symbol
        );
        Ok(reqwest::get(url).await?.json().await?)
    }
}

// ======================================================================
//  WebSocket runner — used by all 3 Binance streams
// ======================================================================

async fn run_binance_conn(
    topics: Vec<String>,
    redis_stream_key: String,
    redis_client: redis::Client,
    metrics: ThroughputMetrics,
    handler: fn(&mut Value, &mut Vec<Vec<u8>>, &ThroughputMetrics) -> Result<()>,
) {
    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        topics.join("/")
    );
    let url = Url::parse(&url).unwrap();

    let mut backoff = Duration::from_millis(RECONNECT_BASE_MS);

    loop {
        match connect_async(&url).await {
            Ok((ws, _)) => {
                println!(
                    "✅ [Binance] Connected WS ({} topics) => {}",
                    topics.len(),
                    redis_stream_key
                );

                let (_, mut read) = ws.split();
                let mut redis_conn = match redis_client.get_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Redis connection failed: {:?}", e);
                        sleep(backoff).await;
                        continue;
                    }
                };

                let mut batch: Vec<Vec<u8>> = Vec::with_capacity(BATCH_FLUSH);

                while let Some(msg) = read.next().await {
                    let msg = match msg {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("[Binance] WS read error: {:?}", e);
                            break;
                        }
                    };

                    if !msg.is_text() {
                        continue;
                    }

                    metrics.incr_recv();

                    let mut bytes = msg.to_text().unwrap().as_bytes().to_vec();
                    let mut v: Value = match simd_json::serde::from_slice(&mut bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    if let Err(e) = handler(&mut v, &mut batch, &metrics) {
                        eprintln!("Binance handler error: {:?}", e);
                    }

                    // flush batch
                    if batch.len() >= BATCH_FLUSH {
                        flush_batch(&mut redis_conn, &redis_stream_key, &mut batch).await;
                    }
                }

                // flush end
                if !batch.is_empty() {
                    flush_batch(&mut redis_conn, &redis_stream_key, &mut batch).await;
                }
            }

            Err(e) => {
                eprintln!(
                    "❌ [Binance] WS connect error: {:?} — retrying {:?}",
                    e, backoff
                );
            }
        }

        // exponential backoff
        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, Duration::from_secs(RECONNECT_MAX_SECS));
    }
}

async fn flush_batch(conn: &mut redis::aio::Connection, key: &str, batch: &mut Vec<Vec<u8>>) {
    let mut pipe = redis::pipe();
    for payload in batch.drain(..) {
        pipe.cmd("XADD")
            .arg(key)
            .arg("MAXLEN")
            .arg("~")
            .arg(MAXLEN)
            .arg("*")
            .arg("data")
            .arg(payload);
    }
    let _: redis::RedisResult<()> = pipe.query_async(conn).await;
}

// ======================================================================
//  Message handlers
// ======================================================================

fn handle_price_msg(
    v: &mut Value,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    let data = match v.get_mut("data") {
        Some(d) => d,
        None => return Ok(()),
    };

    let symbol = data.get("s").and_then(|x| x.as_str()).unwrap_or("");
    if symbol.is_empty() {
        return Ok(());
    }

    let price = data
        .get("c")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let volume = data
        .get("v")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let high = data
        .get("h")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let low = data
        .get("l")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    let timestamp = data
        .get("E")
        .and_then(|x| x.as_u64())
        .unwrap_or_else(current_millis);

    let nd = NormalizedData {
        exchange: "binance".into(),
        symbol: symbol.into(),
        price,
        volume,
        high,
        low,
        timestamp,
    };

    let payload = bincode::serialize(&nd)?;
    batch.push(payload);
    metrics.incr_pub();
    Ok(())
}

fn handle_orderbook_msg(
    v: &mut Value,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    let data = match v.get_mut("data") {
        Some(d) => d,
        None => return Ok(()),
    };

    let symbol = data.get("s").and_then(|x| x.as_str()).unwrap_or("");
    if symbol.is_empty() {
        return Ok(());
    }

    let ob = OrderBookTop {
        exchange: "binance".into(),
        symbol: symbol.into(),
        bid: data
            .get("b")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        ask: data
            .get("a")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        bid_qty: data
            .get("B")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        ask_qty: data
            .get("A")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        timestamp: current_millis(),
    };

    let payload = bincode::serialize(&ob)?;
    batch.push(payload);
    metrics.incr_pub();
    Ok(())
}

fn handle_trade_msg(
    v: &mut Value,
    batch: &mut Vec<Vec<u8>>,
    metrics: &ThroughputMetrics,
) -> Result<()> {
    let data = match v.get_mut("data") {
        Some(d) => d,
        None => return Ok(()),
    };

    let symbol = data.get("s").and_then(|x| x.as_str()).unwrap_or("");
    if symbol.is_empty() {
        return Ok(());
    }

    let price: f64 = data
        .get("p")
        .and_then(|x| x.as_str())
        .and_then(|p| p.parse().ok())
        .unwrap_or(0.0);
    let qty: f64 = data
        .get("q")
        .and_then(|x| x.as_str())
        .and_then(|q| q.parse().ok())
        .unwrap_or(0.0);

    if price <= 0.0 || qty <= 0.0 {
        return Ok(());
    }

    let event = TradeEvent {
        exchange: "binance".into(),
        symbol: symbol.into(),
        price,
        qty,
        side: if data.get("m").and_then(|m| m.as_bool()).unwrap_or(false) {
            "sell".into()
        } else {
            "buy".into()
        },
        timestamp: data
            .get("T")
            .and_then(|t| t.as_u64())
            .unwrap_or_else(current_millis),
    };

    let payload = bincode::serialize(&event)?;
    batch.push(payload);
    metrics.incr_pub();
    Ok(())
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
