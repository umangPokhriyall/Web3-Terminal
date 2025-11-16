use crate::exchange::{
    ExchangeClient, ExchangeHealth, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use crate::metrics::ThroughputMetrics;
use anyhow::Result;
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::connect_async;
use url::Url;

#[derive(Clone)]
pub struct Binance2Collector {
    pub symbols: Vec<String>,
    pub redis_client: redis::Client,
}

// ---- Binance payloads ----
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MiniTicker {
    pub e: String,
    pub E: u64,
    pub s: String,
    pub c: String,
    pub o: String,
    pub h: String,
    pub l: String,
    pub v: String,
    pub q: String,
}

#[derive(Deserialize, Debug)]
struct Binance2StreamWrapper {
    stream: String,
    data: serde_json::Value,
}

#[async_trait::async_trait]
impl ExchangeClient for Binance2Collector {
    fn name(&self) -> &'static str {
        "binance2"
    }

    async fn connect_price_stream(&mut self) -> Result<()> {
        let metrics = ThroughputMetrics::new();
        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            metrics_clone.start_logger("binance2", "price").await;
        });

        let streams: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@miniTicker", s.to_lowercase()))
            .collect();

        let url = Url::parse(&format!(
            "wss://stream.binance.com:9443/stream?streams={}",
            streams.join("/")
        ))?;

        let (ws, _) = connect_async(url).await?;
        println!(
            "✅ [Binance2] Connected price stream ({})",
            self.symbols.join(", ")
        );

        let (_, mut read) = ws.split();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if msg.is_text() {
                metrics.incr_recv(); // count incoming frame
                if let Ok(wrapper) = serde_json::from_str::<Binance2StreamWrapper>(msg.to_text()?) {
                    if let Ok(ticker) = serde_json::from_value::<MiniTicker>(wrapper.data) {
                        let data = NormalizedData {
                            exchange: "binance2".to_string(),
                            symbol: ticker.s.clone(),
                            price: ticker.c.parse().unwrap_or(0.0),
                            volume: ticker.v.parse().unwrap_or(0.0),
                            high: ticker.h.parse().unwrap_or(0.0),
                            low: ticker.l.parse().unwrap_or(0.0),
                            timestamp: ticker.E,
                        };
                        let payload: String = serde_json::to_string(&data)?;
                        let _: () = redis_conn.publish("prices:binance2", payload).await?;
                        metrics.incr_pub(); // count successful publish
                    }
                }
            }
        }
        Ok(())
    }

    async fn connect_orderbook_stream(&mut self) -> Result<()> {
        const RATE_MULTIPLIER: usize = 40; // simulate 40× faster replay

        let metrics = ThroughputMetrics::new();
        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            metrics_clone.start_logger("binance2", "orderbook").await;
        });

        // Separate metrics clones for tasks
        let metrics_for_reader = metrics.clone();
        let metrics_for_replayer = metrics.clone();

        let streams: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();

        let url = Url::parse(&format!(
            "wss://stream.binance.com:9443/stream?streams={}",
            streams.join("/")
        ))?;
        let (ws, _) = connect_async(url).await?;
        println!(
            "✅ [Binance2] Connected orderbook stream ({} symbols)",
            self.symbols.len()
        );

        let (_, mut read) = ws.split();
        let redis_client = self.redis_client.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(10_000);

        // 🧵 Task 1: WebSocket reader → pushes to MPSC
        let reader_task = {
            let tx = tx.clone();
            let metrics = metrics_for_reader.clone();
            tokio::spawn(async move {
                while let Some(msg) = read.next().await {
                    if let Ok(msg) = msg {
                        if msg.is_text() {
                            metrics.incr_recv();
                            let _ = tx.send(msg.to_text().unwrap().to_string()).await;
                        }
                    }
                }
            })
        };

        // 🧵 Task 2: Replay messages at accelerated rate & push to Redis Streams
        let replayer_task = {
            let metrics = metrics_for_replayer.clone();
            let redis_client = redis_client.clone();

            tokio::spawn(async move {
                use tokio::time::{Duration, interval};
                let mut tick = interval(Duration::from_micros(100));

                // Connect to Redis
                let mut redis_conn = match redis_client.get_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("❌ Redis connection error: {:?}", e);
                        return;
                    }
                };

                // Buffer to batch writes (for extra speed)
                let mut batch: Vec<String> = Vec::with_capacity(100);

                while let Some(raw) = rx.recv().await {
                    tick.tick().await;
                    for _ in 0..RATE_MULTIPLIER {
                        if let Ok(wrapper) = serde_json::from_str::<Binance2StreamWrapper>(&raw) {
                            if let (Some(s), Some(b), Some(a)) = (
                                wrapper.data.get("s"),
                                wrapper.data.get("b"),
                                wrapper.data.get("a"),
                            ) {
                                let ob = OrderBookTop {
                                    exchange: "binance2".to_string(),
                                    symbol: s.as_str().unwrap_or("").to_string(),
                                    bid: b.as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                    ask: a.as_str().unwrap_or("0").parse().unwrap_or(0.0),
                                    bid_qty: wrapper
                                        .data
                                        .get("B")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("0")
                                        .parse()
                                        .unwrap_or(0.0),
                                    ask_qty: wrapper
                                        .data
                                        .get("A")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("0")
                                        .parse()
                                        .unwrap_or(0.0),
                                    timestamp: current_millis(),
                                };

                                let payload = serde_json::to_string(&ob).unwrap();
                                batch.push(payload);

                                // When batch is large enough, push all at once
                                if batch.len() >= 50 {
                                    let mut pipe = redis::pipe();
                                    for msg in batch.drain(..) {
                                        // Each message = one entry in the Stream
                                        pipe.cmd("XADD")
                                            .arg("orderbook:binance2") // remove "stream:" prefix
                                            .arg("*")
                                            .arg("data")
                                            .arg(msg);
                                    }
                                    let _: redis::RedisResult<()> =
                                        pipe.query_async(&mut redis_conn).await;
                                }

                                metrics.incr_pub();
                            }
                        }
                    }
                }

                // Flush any remaining messages
                if !batch.is_empty() {
                    let mut pipe = redis::pipe();
                    for msg in batch.drain(..) {
                        pipe.cmd("XADD")
                            .arg("orderbook:binance2") // remove "stream:" prefix
                            .arg("*")
                            .arg("data")
                            .arg(msg);
                    }
                    let _: redis::RedisResult<()> = pipe.query_async(&mut redis_conn).await;
                }
            })
        };

        tokio::try_join!(reader_task, replayer_task)?;
        Ok(())
    }

    async fn connect_trades_stream(&mut self) -> Result<()> {
        let metrics = ThroughputMetrics::new();
        let metrics_clone = metrics.clone();
        tokio::spawn(async move {
            metrics_clone.start_logger("binance2", "trades").await;
        });

        let streams: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("{}@trade", s.to_lowercase()))
            .collect();

        let url = Url::parse(&format!(
            "wss://stream.binance.com:9443/stream?streams={}",
            streams.join("/")
        ))?;

        let (ws, _) = connect_async(url).await?;
        println!(
            "✅ [Binance2] Connected trades stream ({})",
            self.symbols.join(", ")
        );

        let (_, mut read) = ws.split();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if msg.is_text() {
                metrics.incr_recv();
                if let Ok(wrapper) = serde_json::from_str::<Binance2StreamWrapper>(msg.to_text()?) {
                    let d = wrapper.data;
                    let trade = TradeEvent {
                        exchange: "binance2".to_string(),
                        symbol: d
                            .get("s")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        price: d
                            .get("p")
                            .and_then(|x| x.as_str())
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0.0),
                        qty: d
                            .get("q")
                            .and_then(|x| x.as_str())
                            .unwrap_or("0")
                            .parse()
                            .unwrap_or(0.0),
                        side: if d.get("m").and_then(|x| x.as_bool()).unwrap_or(false) {
                            "sell"
                        } else {
                            "buy"
                        }
                        .to_string(),
                        timestamp: d
                            .get("T")
                            .and_then(|x| x.as_u64())
                            .unwrap_or(current_millis()),
                    };
                    let payload = serde_json::to_string(&trade)?;
                    let _: () = redis_conn.publish("trades:binance2", payload).await?;
                    metrics.incr_pub();
                }
            }
        }
        Ok(())
    }

    async fn get_snapshot(&self, symbol: &str) -> Result<OrderBookSnapshot> {
        let url = format!(
            "https://api.binance.com/api/v3/depth?symbol={}&limit=100",
            symbol
        );
        let resp = reqwest::get(&url)
            .await?
            .json::<OrderBookSnapshot>()
            .await?;
        Ok(resp)
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
