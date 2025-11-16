mod config;
mod exchange;
mod exchanges;
mod metrics;

use crate::config::AppConfig;
use crate::exchange::ExchangeClient;
use crate::exchanges::{
    binance::BinanceCollector, binance2::Binance2Collector, bybit::BybitCollector,
    kraken::KrakenCollector,
};
use anyhow::Result;
use futures_util::future;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load("config.yaml")?;
    println!("🚀 Starting collectors with config: {:?}", config);

    let mut tasks = vec![];

    for ex in config.exchanges {
        match ex.name.as_str() {
            "binance" => {
                println!("🟢 Initializing Binance collector...");
                let redis_client = redis::Client::open(config.redis.uri.clone())?;
                let topics = 9;
                let mut collector = BinanceCollector {
                    symbols: ex.symbols.clone(),
                    redis_client,
                    max_topics_per_conn: topics,
                };

                let mut price_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = price_collector.connect_price_stream().await {
                        eprintln!("❌ [Binance] Price stream error: {:?}", e);
                    }
                }));

                let mut orderbook_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = orderbook_collector.connect_orderbook_stream().await {
                        eprintln!("❌ [Binance] Orderbook stream error: {:?}", e);
                    }
                }));

                tasks.push(tokio::spawn(async move {
                    if let Err(e) = collector.connect_trades_stream().await {
                        eprintln!("❌ [Binance] Trades stream error: {:?}", e);
                    }
                }));
            }
            "binance2" => {
                println!("🟢 Initializing Binance2 collector...");
                let redis_client = redis::Client::open(config.redis.uri.clone())?;
                let mut collector = Binance2Collector {
                    symbols: ex.symbols.clone(),
                    redis_client,
                };

                let mut price_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = price_collector.connect_price_stream().await {
                        eprintln!("❌ [Binance2] Price stream error: {:?}", e);
                    }
                }));

                let mut orderbook_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = orderbook_collector.connect_orderbook_stream().await {
                        eprintln!("❌ [Binance2] Orderbook stream error: {:?}", e);
                    }
                }));

                tasks.push(tokio::spawn(async move {
                    if let Err(e) = collector.connect_trades_stream().await {
                        eprintln!("❌ [Binance2] Trades stream error: {:?}", e);
                    }
                }));
            }

            "bybit" => {
                println!("🟢 Initializing Bybit collector...");
                let redis_client = redis::Client::open(config.redis.uri.clone())?;
                let topics = 9;
                let mut collector = BybitCollector {
                    symbols: ex.symbols.clone(),
                    redis_client,
                    max_topics_per_conn: topics,
                };

                let mut price_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = price_collector.connect_price_stream().await {
                        eprintln!("❌ [Bybit] Price stream error: {:?}", e);
                    }
                }));

                let mut orderbook_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = orderbook_collector.connect_orderbook_stream().await {
                        eprintln!("❌ [Bybit] Orderbook stream error: {:?}", e);
                    }
                }));

                tasks.push(tokio::spawn(async move {
                    if let Err(e) = collector.connect_trades_stream().await {
                        eprintln!("❌ [Bybit] Trades stream error: {:?}", e);
                    }
                }));
            }

            "kraken" => {
                println!("🟢 Initializing Kraken collector...");
                let redis_client = redis::Client::open(config.redis.uri.clone())?;
                let mut collector = KrakenCollector {
                    symbols: ex.symbols.clone(),
                    redis_client,
                };

                let mut price_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = price_collector.connect_price_stream().await {
                        eprintln!("❌ [Kraken] Price stream error: {:?}", e);
                    }
                }));

                let mut orderbook_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = orderbook_collector.connect_orderbook_stream().await {
                        eprintln!("❌ [Kraken] Orderbook stream error: {:?}", e);
                    }
                }));

                tasks.push(tokio::spawn(async move {
                    if let Err(e) = collector.connect_trades_stream().await {
                        eprintln!("❌ [Kraken] Trades stream error: {:?}", e);
                    }
                }));
            }
            "hyperliquid" => {
                println!("🟢 Initializing Hyperliquid collector...");
                let redis_client = redis::Client::open(config.redis.uri.clone())?;
                let mut collector = exchanges::hyperliquid::HyperliquidCollector {
                    symbols: ex.symbols.clone(),
                    redis_client,
                };

                let mut price_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = price_collector.connect_price_stream().await {
                        eprintln!("❌ [Hyperliquid] Price stream error: {:?}", e);
                    }
                }));

                let mut orderbook_collector = collector.clone();
                tasks.push(tokio::spawn(async move {
                    if let Err(e) = orderbook_collector.connect_orderbook_stream().await {
                        eprintln!("❌ [Hyperliquid] Orderbook stream error: {:?}", e);
                    }
                }));

                tasks.push(tokio::spawn(async move {
                    if let Err(e) = collector.connect_trades_stream().await {
                        eprintln!("❌ [Hyperliquid] Trades stream error: {:?}", e);
                    }
                }));
            }

            _ => eprintln!("Exchange {} not supported yet.", ex.name),
        }
    }

    future::join_all(tasks).await;
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}
