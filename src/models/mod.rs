use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Deserialize, Debug)]
pub struct TapEvent {
    pub t: i64, // Timestamp
    pub v: i32, // Value/jumlah ketukan
}

#[derive(Deserialize, Debug)]
pub struct BatchPayload {
    pub taps: Vec<TapEvent>,
}

#[derive(Debug, Clone)]
pub struct ValidatedUser {
    pub id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserScoreUpdate {
    pub user_id: i64,
    pub score_increment: i64,
}

#[derive(Debug)]
pub struct TapEventLog {
    pub event_time: DateTime<Utc>,
    pub user_id: i64,
    pub tap_count: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramInitData {
    pub user: Option<TelegramUser>,
    pub auth_date: i64,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelegramUser {
    pub id: i64,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
    pub is_premium: Option<bool>,
}