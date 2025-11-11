use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde_json::Value;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
    vec,
};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Clone)]
pub struct HyperliquidCollector {
    pub symbols: Vec<String>, // e.g. ["BTC", "ETH"]
    pub redis_client: redis::Client,
}

#[async_trait::async_trait]
impl ExchangeClient for HyperliquidCollector {
    fn name(&self) -> &'static str {
        "hyperliquid"
    }

    async fn connect_price_stream(&mut self) -> Result<()> {
        self.connect_combined_stream().await
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

impl HyperliquidCollector {
    pub async fn connect_combined_stream(&mut self) -> Result<()> {
        loop {
            match self.inner_connect().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    eprintln!(
                        "❌ [Hyperliquid] Stream error: {:?}. Reconnecting in 5s...",
                        e
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn inner_connect(&mut self) -> Result<()> {
        let url = Url::parse("wss://api.hyperliquid.xyz/ws")?;
        let (ws, _) = connect_async(url).await?;
        println!(
            "✅ [Hyperliquid] Connected combined stream ({:?})",
            self.symbols
        );

        let (mut write, mut read) = ws.split();
        let write_arc = Arc::new(Mutex::new(write));
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        // Subscribe to orderbook (l2Book), trades, and bbo for each symbol
        for coin in &self.symbols {
            let subs = vec![
                serde_json::json!({ "method": "subscribe", "subscription": { "type": "l2Book", "coin": coin } }),
                serde_json::json!({ "method": "subscribe", "subscription": { "type": "trades", "coin": coin } }),
                serde_json::json!({ "method": "subscribe", "subscription": { "type": "bbo", "coin": coin } }),
            ];
            for sub in subs {
                let mut w = write_arc.lock().await;
                if let Err(e) = w.send(Message::Text(sub.to_string())).await {
                    eprintln!("❌ [Hyperliquid] Failed to send subscription: {:?}", e);
                }
                sleep(Duration::from_millis(200)).await;
            }
        }

        // Keep-alive ping loop
        let ping_writer = Arc::clone(&write_arc);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(50)).await;
                let ping = serde_json::json!({ "method": "ping" });
                if let Err(e) = ping_writer
                    .lock()
                    .await
                    .send(Message::Text(ping.to_string()))
                    .await
                {
                    eprintln!("❌ [Hyperliquid] Ping error: {:?}", e);
                    break;
                }
            }
        });

        // Reader loop
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }

            let txt = msg.to_text()?;
            if txt.contains("\"pong\"") {
                continue;
            }

            let parsed: Value = match serde_json::from_str(txt) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(ch) = parsed.get("channel").and_then(|x| x.as_str()) {
                match ch {
                    "l2Book" => handle_orderbook(&parsed, &mut redis_conn).await?,
                    "trades" => handle_trades(&parsed, &mut redis_conn).await?,
                    "bbo" => handle_bbo(&parsed, &mut redis_conn).await?,
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

// --- HANDLERS ---

async fn handle_orderbook(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(data) = msg.get("data") {
        let coin = data.get("coin").and_then(|x| x.as_str()).unwrap_or("");
        let symbol = normalize_symbol(coin);
        let time = data
            .get("time")
            .and_then(|x| x.as_u64())
            .unwrap_or(current_millis());

        if let Some(levels) = data.get("levels").and_then(|x| x.as_array()) {
            if levels.len() == 2 {
                let empty_vec = Vec::new();
                let bids = levels[0].as_array().unwrap_or(&empty_vec);
                let asks = levels[1].as_array().unwrap_or(&empty_vec);
                let bid = bids
                    .get(0)
                    .and_then(|x| x.get("px"))
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let bid_qty = bids
                    .get(0)
                    .and_then(|x| x.get("sz"))
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let ask = asks
                    .get(0)
                    .and_then(|x| x.get("px"))
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let ask_qty = asks
                    .get(0)
                    .and_then(|x| x.get("sz"))
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);

                if bid > 0.0 && ask > 0.0 {
                    let ob = OrderBookTop {
                        exchange: "hyperliquid".into(),
                        symbol: symbol.to_string(),
                        bid,
                        ask,
                        bid_qty,
                        ask_qty,
                        timestamp: time,
                    };
                    let payload = serde_json::to_string(&ob)?;
                    let _: () = redis.publish("orderbook:hyperliquid", payload).await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_trades(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(data) = msg.get("data").and_then(|x| x.as_array()) {
        for trade in data {
            let coin = trade.get("coin").and_then(|x| x.as_str()).unwrap_or("");
            let symbol = normalize_symbol(coin);
            let price = trade
                .get("px")
                .and_then(|x| x.as_str())
                .and_then(|x| x.parse::<f64>().ok())
                .unwrap_or(0.0);
            let qty = trade
                .get("sz")
                .and_then(|x| x.as_str())
                .and_then(|x| x.parse::<f64>().ok())
                .unwrap_or(0.0);
            let side = trade.get("side").and_then(|x| x.as_str()).unwrap_or("buy");

            if price > 0.0 && qty > 0.0 {
                let ev = TradeEvent {
                    exchange: "hyperliquid".into(),
                    symbol: symbol.to_string(),
                    price,
                    qty,
                    side: side.to_string(),
                    timestamp: current_millis(),
                };
                let payload = serde_json::to_string(&ev)?;
                let _: () = redis.publish("trades:hyperliquid", payload).await?;
            }
        }
    }
    Ok(())
}

async fn handle_bbo(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(data) = msg.get("data") {
        let coin = data.get("coin").and_then(|x| x.as_str()).unwrap_or("");
        let symbol = normalize_symbol(coin);
        if let Some(bbo) = data.get("bbo").and_then(|x| x.as_array()) {
            if bbo.len() == 2 {
                let bid = bbo[0]
                    .get("px")
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let bid_qty = bbo[0]
                    .get("sz")
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let ask = bbo[1]
                    .get("px")
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let ask_qty = bbo[1]
                    .get("sz")
                    .and_then(|x| x.as_str())
                    .and_then(|x| x.parse::<f64>().ok())
                    .unwrap_or(0.0);

                if bid > 0.0 && ask > 0.0 {
                    let nd = NormalizedData {
                        exchange: "hyperliquid".into(),
                        symbol: symbol.to_string(),
                        price: (bid + ask) / 2.0,
                        volume: bid_qty + ask_qty,
                        high: 0.0,
                        low: 0.0,
                        timestamp: current_millis(),
                    };
                    let payload = serde_json::to_string(&nd)?;
                    let _: () = redis.publish("prices:hyperliquid", payload).await?;
                }
            }
        }
    }
    Ok(())
}

// --- HELPERS ---

fn normalize_symbol(s: &str) -> &str {
    match s {
        "BTC" => "BTCUSDT",
        "ETH" => "ETHUSDT",
        "SOL" => "SOLUSDT",
        _ => s,
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
