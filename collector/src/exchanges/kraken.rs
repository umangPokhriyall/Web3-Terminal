// kraken.rs (rewritten, TOB-only, fast, SIMD + bincode + batched streams)

use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use crate::metrics::ThroughputMetrics;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::Value;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

// high-speed JSON + binary
use bincode;
use simd_json;

#[derive(Clone)]
pub struct KrakenCollector {
    pub symbols: Vec<String>, // ["BTC/USD", "ETH/USD", ...]
    pub redis_client: redis::Client,
}

#[async_trait::async_trait]
impl ExchangeClient for KrakenCollector {
    fn name(&self) -> &'static str {
        "kraken"
    }

    async fn connect_price_stream(&mut self) -> Result<()> {
        self.run_resilient().await
    }

    async fn connect_orderbook_stream(&mut self) -> Result<()> {
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

impl KrakenCollector {
    pub async fn run_resilient(&mut self) -> Result<()> {
        loop {
            match self.run().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    eprintln!("❌ [Kraken] Error: {e:?}, retry in 3s...");
                    sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    async fn run(&mut self) -> Result<()> {
        let url = Url::parse("wss://ws.kraken.com/v2")?;
        let (ws, _) = connect_async(url).await?;
        println!("✅ [Kraken] Connected stream: {:?}", self.symbols);

        let (write, mut read) = ws.split();
        let write = Arc::new(Mutex::new(write));

        let redis_client = self.redis_client.clone();
        let mut redis_conn = redis_client.get_async_connection().await?;

        // Metrics
        let metrics = ThroughputMetrics::new();
        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            metrics_clone.start_logger("kraken", "main").await;
        });

        // Subscriptions
        let subs = vec![
            serde_json::json!({"method":"subscribe","params":{"channel":"ticker","symbol":self.symbols}}),
            serde_json::json!({"method":"subscribe","params":{"channel":"trade","symbol":self.symbols}}),
            serde_json::json!({"method":"subscribe","params":{"channel":"book","symbol":self.symbols,"depth":10}}),
        ];

        for sub in subs {
            let mut w = write.lock().await;
            w.send(Message::Text(sub.to_string())).await?;
            sleep(Duration::from_millis(200)).await;
        }

        // batching
        const BATCH: usize = 100;
        const MAXLEN: usize = 10_000;

        let mut batch_prices: Vec<Vec<u8>> = Vec::with_capacity(BATCH);
        let mut batch_orderbook: Vec<Vec<u8>> = Vec::with_capacity(BATCH);
        let mut batch_trades: Vec<Vec<u8>> = Vec::with_capacity(BATCH);

        // === main loop ===
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }

            metrics.incr_recv();

            let txt = msg.to_text()?;
            if txt.contains("heartbeat") {
                continue;
            }

            // SIMD parse
            let mut bytes = txt.as_bytes().to_vec();
            let parsed: Value = match simd_json::serde::from_slice(&mut bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let Some(channel) = parsed.get("channel").and_then(|v| v.as_str()) else {
                continue;
            };

            match channel {
                "ticker" => {
                    handle_ticker(&parsed, &mut batch_prices, &metrics)?;
                    flush_if(
                        &mut batch_prices,
                        "prices:kraken",
                        MAXLEN,
                        BATCH,
                        &mut redis_conn,
                    )
                    .await?;
                }

                "trade" => {
                    handle_trades(&parsed, &mut batch_trades, &metrics)?;
                    flush_if(
                        &mut batch_trades,
                        "trades:kraken",
                        MAXLEN,
                        BATCH,
                        &mut redis_conn,
                    )
                    .await?;
                }

                "book" => {
                    handle_book(&parsed, &mut batch_orderbook, &metrics)?;
                    flush_if(
                        &mut batch_orderbook,
                        "orderbook:kraken",
                        MAXLEN,
                        BATCH,
                        &mut redis_conn,
                    )
                    .await?;
                }

                _ => {}
            }
        }

        // flush everything
        flush_all("prices:kraken", &mut batch_prices, MAXLEN, &mut redis_conn).await?;
        flush_all("trades:kraken", &mut batch_trades, MAXLEN, &mut redis_conn).await?;
        flush_all(
            "orderbook:kraken",
            &mut batch_orderbook,
            MAXLEN,
            &mut redis_conn,
        )
        .await?;

        Ok(())
    }
}

//
// ===== Handlers (super fast, TOB-only) =====
//

fn handle_ticker(msg: &Value, batch: &mut Vec<Vec<u8>>, metrics: &ThroughputMetrics) -> Result<()> {
    let Some(arr) = msg.get("data").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for item in arr {
        let sym_raw = item.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = normalize_symbol(sym_raw);

        let price = item.get("last").and_then(|v| v.as_f64()).unwrap_or(0.0);
        if price <= 0.0 {
            continue;
        }

        let data = NormalizedData {
            exchange: "kraken".into(),
            symbol: symbol.to_string(),
            price,
            volume: item.get("volume").and_then(|v| v.as_f64()).unwrap_or(0.0),
            high: item.get("high").and_then(|v| v.as_f64()).unwrap_or(0.0),
            low: item.get("low").and_then(|v| v.as_f64()).unwrap_or(0.0),
            timestamp: now_ms(),
        };

        if let Ok(payload) = bincode::serialize(&data) {
            batch.push(payload);
        }

        metrics.incr_pub();
    }

    Ok(())
}

fn handle_trades(msg: &Value, batch: &mut Vec<Vec<u8>>, metrics: &ThroughputMetrics) -> Result<()> {
    let Some(arr) = msg.get("data").and_then(|v| v.as_array()) else {
        return Ok(());
    };

    for tr in arr {
        let sym_raw = tr.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = normalize_symbol(sym_raw);

        let price = tr.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let qty = tr.get("qty").and_then(|v| v.as_f64()).unwrap_or(0.0);
        if price <= 0.0 || qty <= 0.0 {
            continue;
        }

        let ev = TradeEvent {
            exchange: "kraken".into(),
            symbol: symbol.to_string(),
            price,
            qty,
            side: tr
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("buy")
                .to_string(),
            timestamp: now_ms(),
        };

        if let Ok(payload) = bincode::serialize(&ev) {
            batch.push(payload);
        }

        metrics.incr_pub();
    }

    Ok(())
}

fn handle_book(msg: &Value, batch: &mut Vec<Vec<u8>>, metrics: &ThroughputMetrics) -> Result<()> {
    let Some(arr) = msg.get("data").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    let _type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");

    for ob in arr {
        let sym_raw = ob.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
        let symbol = normalize_symbol(sym_raw);

        let bids = ob
            .get("bids")
            .and_then(|v| v.as_array())
            .map_or(&[][..], |v| v);

        let asks = ob
            .get("asks")
            .and_then(|v| v.as_array())
            .map_or(&[][..], |v| v);

        let best_bid = bids
            .first()
            .and_then(|x| x.get("price").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);

        let bid_qty = bids
            .first()
            .and_then(|x| x.get("qty").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);

        let best_ask = asks
            .first()
            .and_then(|x| x.get("price").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);

        let ask_qty = asks
            .first()
            .and_then(|x| x.get("qty").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);

        if best_bid == 0.0 || best_ask == 0.0 {
            continue;
        }

        let tob = OrderBookTop {
            exchange: "kraken".into(),
            symbol: symbol.to_string(),
            bid: best_bid,
            ask: best_ask,
            bid_qty,
            ask_qty,
            timestamp: now_ms(),
        };

        if let Ok(payload) = bincode::serialize(&tob) {
            batch.push(payload);
        }

        metrics.incr_pub();
    }

    Ok(())
}

//
// ===== Batch flush helpers =====
//

async fn flush_if(
    batch: &mut Vec<Vec<u8>>,
    stream: &str,
    maxlen: usize,
    limit: usize,
    redis: &mut redis::aio::Connection,
) -> Result<()> {
    if batch.len() < limit {
        return Ok(());
    }
    flush_all(stream, batch, maxlen, redis).await
}

async fn flush_all(
    stream: &str,
    batch: &mut Vec<Vec<u8>>,
    maxlen: usize,
    redis: &mut redis::aio::Connection,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    for msg in batch.drain(..) {
        pipe.cmd("XADD")
            .arg(stream)
            .arg("MAXLEN")
            .arg("~")
            .arg(maxlen)
            .arg("*")
            .arg("data")
            .arg(msg);
    }
    let _: redis::RedisResult<()> = pipe.query_async(redis).await;
    Ok(())
}

//
// ===== Helpers =====
//

fn normalize_symbol(s: &str) -> &str {
    match s {
        "BTC/USD" => "BTCUSDT",
        "ETH/USD" => "ETHUSDT",
        "SOL/USD" => "SOLUSDT",
        "XRP/USD" => "XRPUSDT",
        other => other,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
