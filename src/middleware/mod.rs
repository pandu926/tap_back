use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use url::form_urlencoded;

use crate::{models::ValidatedUser, errors::AppError, AppState};

type HmacSha256 = Hmac<Sha256>;

pub async fn validate_init_data(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, AppError> {
    // Extract Authorization header
    let auth_header = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| AppError::Authentication("Missing authorization header".to_string()))?;

    // Remove "Bearer " prefix if present
    let init_data = if auth_header.starts_with("Bearer ") {
        &auth_header[7..]
    } else {
        auth_header
    };

    // Validate initData
    let user = validate_telegram_init_data(init_data, &state.bot_token)?;
    
    // Add validated user to request extensions
    request.extensions_mut().insert(user);
    
    Ok(next.run(request).await)
}

fn validate_telegram_init_data(init_data: &str, bot_token: &str) -> Result<ValidatedUser, AppError> {
    // Parse URL-encoded data
    let params: HashMap<String, String> = form_urlencoded::parse(init_data.as_bytes())
        .into_owned()
        .collect();

    // Extract hash
    let received_hash = params
        .get("hash")
        .ok_or_else(|| AppError::Authentication("Missing hash in initData".to_string()))?;

    // Create data check string (exclude hash and sort alphabetically)
    let mut data_check_pairs: Vec<String> = params
        .iter()
        .filter(|(key, _)| key.as_str() != "hash")
        .map(|(key, value)| format!("{}={}", key, value))
        .collect();
    
    data_check_pairs.sort();
    let data_check_string = data_check_pairs.join("\n");

    // Create secret key
    let mut mac = HmacSha256::new_from_slice(b"WebAppData")
        .map_err(|e| AppError::Authentication(format!("HMAC key error: {}", e)))?;
    mac.update(bot_token.as_bytes());
    let secret_key = mac.finalize().into_bytes();

    // Calculate expected hash
    let mut mac = HmacSha256::new_from_slice(&secret_key)
        .map_err(|e| AppError::Authentication(format!("HMAC key error: {}", e)))?;
    mac.update(data_check_string.as_bytes());
    let expected_hash = hex::encode(mac.finalize().into_bytes());

    // Compare hashes
    if expected_hash != *received_hash {
        return Err(AppError::Authentication("Invalid initData signature".to_string()));
    }

    // Check auth_date (optional: verify it's not too old)
    let auth_date = params
        .get("auth_date")
        .and_then(|d| d.parse::<i64>().ok())
        .ok_or_else(|| AppError::Authentication("Invalid auth_date".to_string()))?;

    let now = chrono::Utc::now().timestamp();
    if now - auth_date > 86400 { // 24 hours
        return Err(AppError::Authentication("initData too old".to_string()));
    }

    // Parse user data
    let user_json = params
        .get("user")
        .ok_or_else(|| AppError::Authentication("Missing user data".to_string()))?;

    let user: crate::models::TelegramUser = serde_json::from_str(user_json)
        .map_err(|e| AppError::Authentication(format!("Invalid user data: {}", e)))?;

    Ok(ValidatedUser {
        id: user.id,
        username: user.username,
        first_name: Some(user.first_name),
        last_name: user.last_name,
    })
}

// use axum::{
//     extract::{Request, State},
//     http::{HeaderMap},
//     middleware::Next,
//     response::Response,
// };
// use crate::{AppState, models::ValidatedUser};

// pub async fn dummy_user_id_auth(
//     State(_state): State<AppState>,
//     headers: HeaderMap,
//     mut request: Request,
//     next: Next,
// ) -> Result<Response, (axum::http::StatusCode, String)> {
//     let user_id = headers
//         .get("x-user-id")
//         .and_then(|h| h.to_str().ok())
//         .and_then(|s| s.parse::<i64>().ok())
//         .ok_or((
//             axum::http::StatusCode::UNAUTHORIZED,
//             "Missing or invalid X-User-Id header".to_string(),
//         ))?;

//     // Masukkan ke extensions agar bisa diakses di handler
//     request.extensions_mut().insert(ValidatedUser {
//         id: user_id,
//         username: None,
//         first_name: None,
//         last_name: None,
//     });

//     Ok(next.run(request).await)
// }