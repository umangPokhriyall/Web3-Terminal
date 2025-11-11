use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub redis: RedisConfig,
    pub exchanges: Vec<ExchangeConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RedisConfig {
    pub uri: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExchangeConfig {
    pub name: String,
    pub symbols: Vec<String>,
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(path)?;
        let config: AppConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}
