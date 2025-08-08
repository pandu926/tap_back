use chrono::{DateTime, Utc};
use scylla::{value::CqlTimestamp, DeserializeRow};
use serde::{Deserialize, Serialize};

#[derive(Debug, DeserializeRow)]
pub struct Player {
    pub user_id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub auth_date: CqlTimestamp,
    pub level: Option<i32>,
    pub score: Option<i64>,
    pub energy: Option<i32>,
    pub max_energy: Option<i32>,
    pub energy_last_recharged: Option<CqlTimestamp>,
    pub tap_value: Option<i32>,
    pub energy_recharge_rate: Option<i32>,
    pub referral_code: Option<String>,
    pub referred_by_id: Option<i64>,
    pub last_seen: Option<CqlTimestamp>,
}

pub struct CreatePlayerRequest {
    pub user_id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub referral_code: Option<String>,
    pub referred_by_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePlayerRequest {
    pub user_id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub level: Option<i32>,
    pub energy: Option<i32>,
    pub max_energy: Option<i32>,
    pub tap_value: Option<i32>,
    pub energy_recharge_rate: Option<i32>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerResponse {
    pub user_id: i64,
    pub username: Option<String>,
    pub first_name: Option<String>,
    pub score: i64,
    pub level: i32,
    pub energy: i32,
    pub max_energy: i32,
    pub tap_value: i32,
    pub energy_recharge_rate: i32,
    pub referral_code: Option<String>,
    pub last_seen: Option<DateTime<Utc>>,
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
    pub new_score: i64,
}
