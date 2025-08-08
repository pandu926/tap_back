use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, TokenData, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String, // biasanya ID user
    exp: usize,  // UNIX timestamp (detik)
}

pub struct AuthService {
    secret: String,
}

impl AuthService {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
        }
    }

    pub fn generate_jwt(&self, user_id: i64) -> Result<(String, usize)> {
        let expiration = Utc::now()
            .checked_add_signed(Duration::days(7))
            .expect("valid timestamp")
            .timestamp() as usize;

        let claims = Claims {
            sub: user_id.to_string(),
            exp: expiration,
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )?;

        Ok((token, expiration))
    }

    pub fn decode_jwt(&self, token: &str) -> Result<i64> {
        let validation = Validation::default();

        let token_data: TokenData<Claims> = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )?;

        let user_id = token_data
            .claims
            .sub
            .parse::<i64>()
            .map_err(|_| anyhow!("Failed to parse user_id from token claims"))?;

        Ok(user_id)
    }
}
