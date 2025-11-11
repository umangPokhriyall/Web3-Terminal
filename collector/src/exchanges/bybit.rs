use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
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
use tokio_tungstenite::connect_async;
use url::Url;

/// Bybit collector (public spot channels)
#[derive(Clone)]
pub struct BybitCollector {
    pub symbols: Vec<String>, // e.g. ["BTCUSDT", "ETHUSDT"]
    pub redis_client: redis::Client,
}

#[derive(Debug, Serialize, Deserialize)]
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

    /// Price/ticker stream -> publish to prices:bybit
    async fn connect_price_stream(&mut self) -> Result<()> {
        let url = Url::parse("wss://stream.bybit.com/v5/public/spot")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!("✅ [Bybit] Connected price stream ({:?})", self.symbols);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Subscribe to tickers
        let topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("tickers.{}", s))
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": topics});
        {
            let mut writer = write.lock().await;
            writer
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    sub.to_string(),
                ))
                .await?;
        }

        // Ping every 20s
        {
            let w = Arc::clone(&write);
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(20)).await;
                    let ping = serde_json::json!({"op": "ping"});
                    let mut writer = w.lock().await;
                    if let Err(e) = writer
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            ping.to_string(),
                        ))
                        .await
                    {
                        eprintln!("❌ [Bybit] Price ping error: {:?}", e);
                        break;
                    }
                }
            });
        }

        let mut redis_conn = self.redis_client.get_async_connection().await?;
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }
            let txt = msg.to_text()?;
            if let Ok(v) = serde_json::from_str::<Value>(txt) {
                if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                    if topic.starts_with("tickers.") {
                        if let Some(data) = v.get("data") {
                            if let Ok(td) = serde_json::from_value::<BybitTickerData>(data.clone())
                            {
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
                                    high: td
                                        .high_24h
                                        .as_deref()
                                        .unwrap_or("0")
                                        .parse()
                                        .unwrap_or(0.0),
                                    low: td
                                        .low_24h
                                        .as_deref()
                                        .unwrap_or("0")
                                        .parse()
                                        .unwrap_or(0.0),
                                    timestamp: v
                                        .get("ts")
                                        .and_then(|x| x.as_u64())
                                        .unwrap_or(current_millis()),
                                };
                                let payload = serde_json::to_string(&nd)?;
                                let _: () = redis_conn.publish("prices:bybit", payload).await?;
                                println!(
                                    "[Bybit] {} | Price: {} | Vol: {}",
                                    nd.symbol, nd.price, nd.volume
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Orderbook stream (level 1)
    async fn connect_orderbook_stream(&mut self) -> Result<()> {
        let url = Url::parse("wss://stream.bybit.com/v5/public/spot")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!("✅ [Bybit] Connected orderbook stream ({:?})", self.symbols);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        let topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("orderbook.1.{}", s))
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": topics});
        {
            let mut writer = write.lock().await;
            writer
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    sub.to_string(),
                ))
                .await?;
        }

        // Ping every 20s
        {
            let w = Arc::clone(&write);
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(20)).await;
                    let ping = serde_json::json!({"op": "ping"});
                    let mut writer = w.lock().await;
                    if let Err(e) = writer
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            ping.to_string(),
                        ))
                        .await
                    {
                        eprintln!("❌ [Bybit] Orderbook ping error: {:?}", e);
                        break;
                    }
                }
            });
        }

        let mut redis_conn = self.redis_client.get_async_connection().await?;
        let mut best: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }
            let txt = msg.to_text()?;
            if let Ok(v) = serde_json::from_str::<Value>(txt) {
                if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                    if topic.starts_with("orderbook.1.") {
                        if let Some(data) = v.get("data") {
                            let symbol = topic.trim_start_matches("orderbook.1.").to_string();
                            let bids = data
                                .get("b")
                                .and_then(|b| b.as_array())
                                .cloned()
                                .unwrap_or_default();
                            let asks = data
                                .get("a")
                                .and_then(|a| a.as_array())
                                .cloned()
                                .unwrap_or_default();
                            if bids.is_empty() || asks.is_empty() {
                                continue;
                            }

                            let (bid, bid_qty) = (
                                bids[0][0]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0),
                                bids[0][1]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0),
                            );
                            let (ask, ask_qty) = (
                                asks[0][0]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0),
                                asks[0][1]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0),
                            );

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
                                    .unwrap_or(current_millis()),
                            };
                            let payload = serde_json::to_string(&ob)?;
                            let _: () = redis_conn.publish("orderbook:bybit", payload).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Trades stream
    async fn connect_trades_stream(&mut self) -> Result<()> {
        let url = Url::parse("wss://stream.bybit.com/v5/public/spot")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!("✅ [Bybit] Connected trades stream ({:?})", self.symbols);

        let (write, mut read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        let topics: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("publicTrade.{}", s))
            .collect();
        let sub = serde_json::json!({"op": "subscribe", "args": topics});
        {
            let mut writer = write.lock().await;
            writer
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    sub.to_string(),
                ))
                .await?;
        }

        // Ping every 20s
        {
            let w = Arc::clone(&write);
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(20)).await;
                    let ping = serde_json::json!({"op": "ping"});
                    let mut writer = w.lock().await;
                    if let Err(e) = writer
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            ping.to_string(),
                        ))
                        .await
                    {
                        eprintln!("❌ [Bybit] Trades ping error: {:?}", e);
                        break;
                    }
                }
            });
        }

        let mut redis_conn = self.redis_client.get_async_connection().await?;
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }
            let txt = msg.to_text()?;
            if let Ok(v) = serde_json::from_str::<Value>(txt) {
                if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                    if topic.starts_with("publicTrade.") {
                        if let Some(arr) = v.get("data").and_then(|x| x.as_array()) {
                            for trade in arr {
                                let symbol = trade
                                    .get("s")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let side = trade
                                    .get("S")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("Buy")
                                    .to_lowercase();
                                let qty = trade
                                    .get("v")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);
                                let price = trade
                                    .get("p")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);
                                let tstamp = trade
                                    .get("T")
                                    .and_then(|x| x.as_u64())
                                    .unwrap_or(current_millis());

                                let event = TradeEvent {
                                    exchange: "bybit".into(),
                                    symbol: symbol.clone(),
                                    price,
                                    qty,
                                    side,
                                    timestamp: tstamp,
                                };
                                let payload = serde_json::to_string(&event)?;
                                let _: () = redis_conn.publish("trades:bybit", payload).await?;
                            }
                        }
                    }
                }
            }
        }
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

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
