use crate::exchange::{
    ExchangeClient, ExchangeHealth, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use anyhow::Result;
use futures_util::{StreamExt, stream::SelectAll};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    select,
    time::{Duration, Instant, sleep},
};
use tokio_tungstenite::connect_async;
use url::Url;

#[derive(Clone)]
pub struct BinanceCollector {
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
struct BinanceStreamWrapper {
    stream: String,
    data: serde_json::Value,
}

#[async_trait::async_trait]
impl ExchangeClient for BinanceCollector {
    fn name(&self) -> &'static str {
        "binance"
    }

    async fn connect_price_stream(&mut self) -> Result<()> {
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
            "✅ [Binance] Connected price stream ({})",
            self.symbols.join(", ")
        );

        let (_, mut read) = ws.split();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if msg.is_text() {
                if let Ok(wrapper) = serde_json::from_str::<BinanceStreamWrapper>(msg.to_text()?) {
                    if let Ok(ticker) = serde_json::from_value::<MiniTicker>(wrapper.data) {
                        let data = NormalizedData {
                            exchange: "binance".to_string(),
                            symbol: ticker.s.clone(),
                            price: ticker.c.parse().unwrap_or(0.0),
                            volume: ticker.v.parse().unwrap_or(0.0),
                            high: ticker.h.parse().unwrap_or(0.0),
                            low: ticker.l.parse().unwrap_or(0.0),
                            timestamp: ticker.E,
                        };
                        let payload = serde_json::to_string(&data)?;
                        let _: () = redis_conn.publish("prices:binance", payload).await?;
                        println!(
                            "[Binance] {} | Price: {} | Vol: {}",
                            data.symbol, data.price, data.volume
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn connect_orderbook_stream(&mut self) -> Result<()> {
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
            "✅ [Binance] Connected orderbook stream ({})",
            self.symbols.join(", ")
        );

        let (_, mut read) = ws.split();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if msg.is_text() {
                if let Ok(wrapper) = serde_json::from_str::<BinanceStreamWrapper>(msg.to_text()?) {
                    let data = wrapper.data;
                    if let (Some(s), Some(b), Some(a)) =
                        (data.get("s"), data.get("b"), data.get("a"))
                    {
                        let ob = OrderBookTop {
                            exchange: "binance".to_string(),
                            symbol: s.as_str().unwrap_or("").to_string(),
                            bid: b.as_str().unwrap_or("0").parse().unwrap_or(0.0),
                            ask: a.as_str().unwrap_or("0").parse().unwrap_or(0.0),
                            bid_qty: data
                                .get("B")
                                .and_then(|x| x.as_str())
                                .unwrap_or("0")
                                .parse()
                                .unwrap_or(0.0),
                            ask_qty: data
                                .get("A")
                                .and_then(|x| x.as_str())
                                .unwrap_or("0")
                                .parse()
                                .unwrap_or(0.0),
                            timestamp: current_millis(),
                        };
                        let payload = serde_json::to_string(&ob)?;
                        let _: () = redis_conn.publish("orderbook:binance", payload).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn connect_trades_stream(&mut self) -> Result<()> {
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
            "✅ [Binance] Connected trades stream ({})",
            self.symbols.join(", ")
        );

        let (_, mut read) = ws.split();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if msg.is_text() {
                if let Ok(wrapper) = serde_json::from_str::<BinanceStreamWrapper>(msg.to_text()?) {
                    let d = wrapper.data;
                    let trade = TradeEvent {
                        exchange: "binance".to_string(),
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
                    let _: () = redis_conn.publish("trades:binance", payload).await?;
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
