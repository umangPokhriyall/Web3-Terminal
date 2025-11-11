// src/exchanges/coinbase.rs
use crate::exchange::{
    ExchangeClient, NormalizedData, OrderBookSnapshot, OrderBookTop, TradeEvent,
};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::connect_async;
use url::Url;

#[derive(Clone)]
pub struct CoinbaseCollector {
    pub symbols: Vec<String>, // e.g. ["BTC-USD", "ETH-USD"]
    pub redis_client: redis::Client,
}

#[async_trait::async_trait]
impl ExchangeClient for CoinbaseCollector {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    /// ✅ Orderbook collector: REST preload + WS level2 stream
    async fn connect_orderbook_stream(&mut self) -> Result<()> {
        println!(
            "✅ [Coinbase] Starting orderbook collector for: {:?}",
            self.symbols
        );

        // Create HTTP + Redis clients once
        let http = Client::new();
        let mut redis_conn = self.redis_client.get_async_connection().await?;

        // Store best quotes in memory
        let mut best_quotes: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        // symbol -> (bid, ask, bid_qty, ask_qty)

        // ---------------------------
        // 1️⃣ REST snapshot preload
        // ---------------------------
        for symbol in &self.symbols {
            let url = format!(
                "https://api.exchange.coinbase.com/products/{}/book?level=2",
                symbol
            );
            match http
                .get(&url)
                .header("Accept", "application/json")
                .header("User-Agent", "web3-terminal/collector")
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(json) = resp.json::<Value>().await {
                            let bids = json
                                .get("bids")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();
                            let asks = json
                                .get("asks")
                                .and_then(|v| v.as_array())
                                .cloned()
                                .unwrap_or_default();

                            if !bids.is_empty() && !asks.is_empty() {
                                let best_bid = bids[0][0]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);
                                let best_bid_qty = bids[0][1]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);
                                let best_ask = asks[0][0]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);
                                let best_ask_qty = asks[0][1]
                                    .as_str()
                                    .unwrap_or("0")
                                    .parse::<f64>()
                                    .unwrap_or(0.0);

                                best_quotes.insert(
                                    symbol.clone(),
                                    (best_bid, best_ask, best_bid_qty, best_ask_qty),
                                );

                                println!(
                                    "🟦 [Coinbase][REST snapshot] {} top bid={} ({}) ask={} ({})",
                                    symbol, best_bid, best_bid_qty, best_ask, best_ask_qty
                                );

                                let ob = OrderBookTop {
                                    exchange: "coinbase".to_string(),
                                    symbol: symbol.clone(),
                                    bid: best_bid,
                                    ask: best_ask,
                                    bid_qty: best_bid_qty,
                                    ask_qty: best_ask_qty,
                                    timestamp: current_millis(),
                                };
                                let payload = serde_json::to_string(&ob)?;
                                if let Err(e) = redis_conn
                                    .publish::<_, _, ()>("orderbook:coinbase", &payload)
                                    .await
                                {
                                    eprintln!(
                                        "⚠️ [Coinbase][Redis] Publish failed for {}: {:?}",
                                        symbol, e
                                    );
                                } else {
                                    println!("📤 [Coinbase][Redis] Published snapshot {}", symbol);
                                }
                            }
                        }
                    } else {
                        eprintln!(
                            "⚠️ [Coinbase] REST snapshot failed for {}: {}",
                            symbol,
                            resp.status()
                        );
                    }
                }
                Err(e) => {
                    eprintln!("⚠️ [Coinbase] REST error for {}: {:?}", symbol, e);
                }
            }

            // spacing between requests
            sleep(Duration::from_millis(100)).await;
        }

        // ---------------------------
        // 2️⃣ WebSocket: level2 diffs
        // ---------------------------
        let url = Url::parse("wss://ws-feed.exchange.coinbase.com")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!("✅ [Coinbase] WS connected (level2). Subscribing...");

        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "channels": [{ "name": "level2", "product_ids": self.symbols }]
        });

        let (mut write, mut read) = ws_stream.split();
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string(),
            ))
            .await?;
        println!("✅ [Coinbase] Sent level2 subscribe message");

        while let Some(msg) = read.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("❌ [Coinbase] WS read error: {:?}", e);
                    continue;
                }
            };

            if !msg.is_text() {
                continue;
            }

            let text = msg.to_text()?;
            let parsed: Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(_) => continue,
            };

            match parsed.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "subscriptions" => {
                    println!("🟢 [Coinbase] Subscriptions confirmed.");
                }
                "snapshot" => {
                    let product_id = parsed
                        .get("product_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let bids = parsed
                        .get("bids")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let asks = parsed
                        .get("asks")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    if !bids.is_empty() && !asks.is_empty() {
                        let best_bid = bids[0][0]
                            .as_str()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        let best_bid_qty = bids[0][1]
                            .as_str()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        let best_ask = asks[0][0]
                            .as_str()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        let best_ask_qty = asks[0][1]
                            .as_str()
                            .unwrap_or("0")
                            .parse::<f64>()
                            .unwrap_or(0.0);

                        best_quotes.insert(
                            product_id.to_string(),
                            (best_bid, best_ask, best_bid_qty, best_ask_qty),
                        );

                        let ob = OrderBookTop {
                            exchange: "coinbase".to_string(),
                            symbol: product_id.to_string(),
                            bid: best_bid,
                            ask: best_ask,
                            bid_qty: best_bid_qty,
                            ask_qty: best_ask_qty,
                            timestamp: current_millis(),
                        };
                        let payload = serde_json::to_string(&ob)?;
                        let _: () = redis_conn.publish("orderbook:coinbase", payload).await?;
                        println!(
                            "[Coinbase][WS snapshot] {} | Bid: {} ({}) | Ask: {} ({})",
                            ob.symbol, ob.bid, ob.bid_qty, ob.ask, ob.ask_qty
                        );
                    }
                }
                "l2update" => {
                    let product_id = parsed
                        .get("product_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let changes = parsed
                        .get("changes")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    let (mut bid, mut ask, mut bid_qty, mut ask_qty) = best_quotes
                        .get(product_id)
                        .copied()
                        .unwrap_or((0.0, 0.0, 0.0, 0.0));

                    for c in changes {
                        if let Some(arr) = c.as_array() {
                            let side = arr.get(0).and_then(|v| v.as_str()).unwrap_or("");
                            let price = arr
                                .get(1)
                                .and_then(|v| v.as_str())
                                .unwrap_or("0")
                                .parse::<f64>()
                                .unwrap_or(0.0);
                            let size = arr
                                .get(2)
                                .and_then(|v| v.as_str())
                                .unwrap_or("0")
                                .parse::<f64>()
                                .unwrap_or(0.0);

                            match side {
                                "buy" if price >= bid => {
                                    bid = price;
                                    bid_qty = size;
                                }
                                "sell" if ask == 0.0 || price <= ask => {
                                    ask = price;
                                    ask_qty = size;
                                }
                                _ => {}
                            }
                        }
                    }

                    best_quotes.insert(product_id.to_string(), (bid, ask, bid_qty, ask_qty));

                    let ob = OrderBookTop {
                        exchange: "coinbase".to_string(),
                        symbol: product_id.to_string(),
                        bid,
                        ask,
                        bid_qty,
                        ask_qty,
                        timestamp: current_millis(),
                    };
                    let payload = serde_json::to_string(&ob)?;
                    let _: () = redis_conn.publish("orderbook:coinbase", payload).await?;
                    println!(
                        "[Coinbase] {} | Bid: {} ({}) | Ask: {} ({})",
                        ob.symbol, ob.bid, ob.bid_qty, ob.ask, ob.ask_qty
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// ✅ Ticker (price + 24h volume)
    async fn connect_price_stream(&mut self) -> Result<()> {
        let url = Url::parse("wss://ws-feed.exchange.coinbase.com")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!("✅ [Coinbase] WS connected (ticker) for {:?}", self.symbols);

        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "channels": [{ "name": "ticker", "product_ids": self.symbols }]
        });

        let (mut write, mut read) = ws_stream.split();
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string(),
            ))
            .await?;

        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }
            let text = msg.to_text()?;
            let parsed: Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if parsed.get("type").and_then(|t| t.as_str()) == Some("ticker") {
                if let (Some(product_id), Some(price), Some(volume_24h)) = (
                    parsed.get("product_id").and_then(|v| v.as_str()),
                    parsed.get("price").and_then(|v| v.as_str()),
                    parsed.get("volume_24h").and_then(|v| v.as_str()),
                ) {
                    let data = NormalizedData {
                        exchange: "coinbase".to_string(),
                        symbol: product_id.to_string(),
                        price: price.parse().unwrap_or(0.0),
                        volume: volume_24h.parse().unwrap_or(0.0),
                        high: 0.0,
                        low: 0.0,
                        timestamp: current_millis(),
                    };
                    let payload = serde_json::to_string(&data)?;
                    let _: () = redis_conn.publish("prices:coinbase", payload).await?;
                    println!(
                        "[Coinbase] {} | Price: {} | Vol(24h): {}",
                        data.symbol, data.price, data.volume
                    );
                }
            }
        }

        Ok(())
    }

    /// ✅ Trades stream
    async fn connect_trades_stream(&mut self) -> Result<()> {
        let url = Url::parse("wss://ws-feed.exchange.coinbase.com")?;
        let (ws_stream, _) = connect_async(url).await?;
        println!(
            "✅ [Coinbase] WS connected (matches) for {:?}",
            self.symbols
        );

        let subscribe_msg = serde_json::json!({
            "type": "subscribe",
            "channels": [{ "name": "matches", "product_ids": self.symbols }]
        });

        let (mut write, mut read) = ws_stream.split();
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe_msg.to_string(),
            ))
            .await?;

        let mut redis_conn = self.redis_client.get_async_connection().await?;

        while let Some(msg) = read.next().await {
            let msg = msg?;
            if !msg.is_text() {
                continue;
            }
            let text = msg.to_text()?;
            let parsed: Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if parsed.get("type").and_then(|v| v.as_str()) == Some("match") {
                if let (Some(product_id), Some(price), Some(size), Some(side)) = (
                    parsed.get("product_id").and_then(|v| v.as_str()),
                    parsed.get("price").and_then(|v| v.as_str()),
                    parsed.get("size").and_then(|v| v.as_str()),
                    parsed.get("side").and_then(|v| v.as_str()),
                ) {
                    let trade = TradeEvent {
                        exchange: "coinbase".to_string(),
                        symbol: product_id.to_string(),
                        price: price.parse().unwrap_or(0.0),
                        qty: size.parse().unwrap_or(0.0),
                        side: side.to_string(),
                        timestamp: current_millis(),
                    };
                    let payload = serde_json::to_string(&trade)?;
                    let _: () = redis_conn.publish("trades:coinbase", payload).await?;
                    println!(
                        "[Coinbase][match] {} | {} {} @ {}",
                        trade.symbol, trade.side, trade.qty, trade.price
                    );
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
