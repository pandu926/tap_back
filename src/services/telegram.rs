use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use crate::errors::AppError;

type HmacSha256 = Hmac<Sha256>;

pub struct TelegramService {
    bot_token: String,
}

impl TelegramService {
    /// Constructor: langsung pakai bot_token dari env
    pub fn new(bot_token: String) -> Self {
        Self { bot_token }
    }

    /// Verifikasi init data Telegram
    pub fn verify_init_data(&self, init_data: &str) -> Result<bool, AppError> {
        let mut params = HashMap::new();
        for pair in init_data.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(
                    k.to_string(),
                    urlencoding::decode(v)
                        .map_err(|e| AppError::Internal(e.into()))?
                        .to_string(),
                );
            }
        }

        let received_hash = params
            .remove("hash")
            .ok_or_else(|| AppError::Auth("Hash not found in init data".to_string()))?;

        let mut sorted: Vec<_> = params.iter().collect();
        sorted.sort_by_key(|(k, _)| *k);

        let data_check_string = sorted
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("\n");

        // 1. Buat secret key dari token
        let mut mac = HmacSha256::new_from_slice(b"WebAppData")
            .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        mac.update(self.bot_token.as_bytes());
        let secret_key = mac.finalize().into_bytes();

        // 2. Hitung hash dari data_check_string
        let mut mac = HmacSha256::new_from_slice(&secret_key)
            .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;
        mac.update(data_check_string.as_bytes());
        let calc_hash = hex::encode(mac.finalize().into_bytes());

        Ok(calc_hash == received_hash)
    }
}
