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
};
use tokio::{
    sync::Mutex,
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

#[derive(Clone)]
pub struct KrakenCollector {
    pub symbols: Vec<String>, // e.g. ["XBT/USD", "ETH/USD"]
    pub redis_client: redis::Client,
}

#[async_trait::async_trait]
impl ExchangeClient for KrakenCollector {
    fn name(&self) -> &'static str {
        "kraken"
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

impl KrakenCollector {
    pub async fn connect_combined_stream(&mut self) -> Result<()> {
        loop {
            match self.inner_connect().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    eprintln!("❌ [Kraken] Stream error: {:?}. Reconnecting in 5s...", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn inner_connect(&mut self) -> Result<()> {
        let url = Url::parse("wss://ws.kraken.com/v2")?;
        let (ws, _) = connect_async(url).await?;
        println!("✅ [Kraken] Connected combined stream ({:?})", self.symbols);

        let (write, mut read) = ws.split();
        let write = Arc::new(Mutex::new(write));
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        // Subscriptions: ticker, trade, book
        let subs = vec![
            serde_json::json!({"method":"subscribe","params":{"channel":"ticker","symbol":self.symbols}}),
            serde_json::json!({"method":"subscribe","params":{"channel":"trade","symbol":self.symbols}}),
            serde_json::json!({"method":"subscribe","params":{"channel":"book","symbol":self.symbols,"depth":10}}),
        ];

        for sub in subs {
            let mut w = write.lock().await;
            w.send(Message::Text(sub.to_string())).await?;
            sleep(Duration::from_millis(500)).await;
        }

        // Ping loop
        let ping_writer = write.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(25)).await;
                let ping = serde_json::json!({"method": "ping"});
                if let Err(e) = ping_writer
                    .lock()
                    .await
                    .send(Message::Text(ping.to_string()))
                    .await
                {
                    eprintln!("❌ [Kraken] ping error: {:?}", e);
                    break;
                }
            }
        });

        // Reader
        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }

            let txt = msg.to_text()?;
            if txt.contains("\"heartbeat\"") {
                continue;
            }

            let parsed: Value = match serde_json::from_str(txt) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(ch) = parsed.get("channel").and_then(|x| x.as_str()) {
                match ch {
                    "ticker" => handle_ticker(&parsed, &mut redis_conn).await?,
                    "trade" => handle_trade(&parsed, &mut redis_conn).await?,
                    "book" => handle_orderbook(&parsed, &mut redis_conn).await?,
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

// --- HANDLERS ---

async fn handle_ticker(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(arr) = msg.get("data").and_then(|x| x.as_array()) {
        for item in arr {
            let raw_symbol = item.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let symbol = normalize_symbol(raw_symbol);
            let price = item.get("last").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let volume = item.get("volume").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let high = item.get("high").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let low = item.get("low").and_then(|x| x.as_f64()).unwrap_or(0.0);

            if !symbol.is_empty() && price > 0.0 {
                let data = NormalizedData {
                    exchange: "kraken".into(),
                    symbol: symbol.to_string(),
                    price,
                    volume,
                    high,
                    low,
                    timestamp: current_millis(),
                };
                let payload = serde_json::to_string(&data)?;
                let _: () = redis.publish("prices:kraken", payload).await?;
            }
        }
    }
    Ok(())
}

async fn handle_trade(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(arr) = msg.get("data").and_then(|x| x.as_array()) {
        for trade in arr {
            let raw_symbol = trade.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let symbol = normalize_symbol(raw_symbol);
            let price = trade.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let qty = trade.get("qty").and_then(|x| x.as_f64()).unwrap_or(0.0);
            let side = trade.get("side").and_then(|x| x.as_str()).unwrap_or("buy");

            if !symbol.is_empty() && price > 0.0 && qty > 0.0 {
                let event = TradeEvent {
                    exchange: "kraken".into(),
                    symbol: symbol.to_string(),
                    price,
                    qty,
                    side: side.to_string(),
                    timestamp: current_millis(),
                };
                let payload = serde_json::to_string(&event)?;
                let _: () = redis.publish("trades:kraken", payload).await?;
            }
        }
    }
    Ok(())
}

async fn handle_orderbook(msg: &Value, redis: &mut redis::aio::Connection) -> Result<()> {
    if let Some(arr) = msg.get("data").and_then(|x| x.as_array()) {
        for ob in arr {
            let raw_symbol = ob.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
            let symbol = normalize_symbol(raw_symbol);

            let bid = ob
                .get("bids")
                .and_then(|b| b.get(0))
                .and_then(|x| x.get("price"))
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            let bid_qty = ob
                .get("bids")
                .and_then(|b| b.get(0))
                .and_then(|x| x.get("qty"))
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            let ask = ob
                .get("asks")
                .and_then(|a| a.get(0))
                .and_then(|x| x.get("price"))
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);
            let ask_qty = ob
                .get("asks")
                .and_then(|a| a.get(0))
                .and_then(|x| x.get("qty"))
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0);

            if !symbol.is_empty() && (bid > 0.0 || ask > 0.0) {
                let data = OrderBookTop {
                    exchange: "kraken".into(),
                    symbol: symbol.to_string(),
                    bid,
                    ask,
                    bid_qty,
                    ask_qty,
                    timestamp: current_millis(),
                };
                let payload = serde_json::to_string(&data)?;
                let _: () = redis.publish("orderbook:kraken", payload).await?;
            }
        }
    }
    Ok(())
}

// --- HELPERS ---

fn normalize_symbol(s: &str) -> &str {
    match s {
        "BTC/USD" => "BTCUSDT",
        "ETH/USD" => "ETHUSDT",
        "SOL/USD" => "SOLUSDT",
        "XRP/USD" => "XRPUSDT",
        _ => s,
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
