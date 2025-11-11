use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NormalizedData {
    pub exchange: String,
    pub symbol: String,
    pub price: f64,
    pub volume: f64,
    pub high: f64,
    pub low: f64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderBookTop {
    pub exchange: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TradeEvent {
    pub exchange: String,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub side: String, // "buy" or "sell"
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExchangeHealth {
    pub exchange: String,
    pub latency_ms: u64,
    pub last_message_ts: u64,
    pub status: String, // "OK", "STALE", "DISCONNECTED"
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrderBookSnapshot {
    pub lastUpdateId: u64,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    fn name(&self) -> &'static str;

    async fn connect_price_stream(&mut self) -> Result<()>;
    async fn connect_orderbook_stream(&mut self) -> Result<()>;
    async fn connect_trades_stream(&mut self) -> Result<()>;

    async fn get_snapshot(&self, symbol: &str) -> Result<OrderBookSnapshot>;
}
