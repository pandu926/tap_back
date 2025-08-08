use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub database_url: String,
    pub telegram_bot_token: String,
    pub jwt_secret: String,
    pub port: u16,
    pub cors_origin: String,
    pub jwt_expiry_hours: i64,
    pub redis_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Self {
            database_url: std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            telegram_bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .expect("TELEGRAM_BOT_TOKEN must be set"),
            jwt_secret: std::env::var("JWT_SECRET")
                .unwrap_or_else(|_| "your-secret-key-change-this".to_string()),
            port: std::env::var("PORT")
                .unwrap_or_else(|_| "3000".to_string())
                .parse()
                .expect("PORT must be a valid number"),
            cors_origin: std::env::var("CORS_ORIGIN")
                .unwrap_or_else(|_| "https://sdsd-ashy.vercel.app".to_string()),
            jwt_expiry_hours: std::env::var("JWT_EXPIRY_HOURS")
                .unwrap_or_else(|_| "24".to_string())
                .parse()
                .unwrap_or(24),
            redis_url: std::env::var("REDIS_URL").expect("redis not"),
        })
    }
}
