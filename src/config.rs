use anyhow::Result;
use config::{Environment, File};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub database_path: String,
    pub max_concurrent_queries: usize,
    pub max_query_time_ms: u64,
    pub listen_address: String,
    pub listen_port: u16,
}

impl Config {
    pub fn load() -> Result<Self> {
        let mut builder = config::Config::builder();

        // Add configuration sources
        builder = builder.add_source(File::with_name("config").required(false));
        builder = builder.add_source(Environment::with_prefix("SQLITE_API").separator("__"));

        // Build configuration
        let config = builder.build()?;

        // Try to deserialize, falling back to defaults if not found
        let config: Self = match config.try_deserialize() {
            Ok(cfg) => cfg,
            Err(_) => {
                // Provide defaults if config file/environment variables are missing
                Self {
                    database_path: "data.db".to_string(),
                    max_concurrent_queries: 4,
                    max_query_time_ms: 5000,
                    listen_address: "0.0.0.0".to_string(),
                    listen_port: 8080,
                }
            }
        };

        // Validate configuration
        if config.max_concurrent_queries == 0 {
            return Err(anyhow::anyhow!("max_concurrent_queries must be at least 1"));
        }

        if !Path::new(&config.database_path).exists() {
            return Err(anyhow::anyhow!(
                "Database file {} does not exist",
                config.database_path
            ));
        }

        Ok(config)
    }
}
