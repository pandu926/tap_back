use chrono::{DateTime, Utc};
use scylla::{value::CqlTimestamp, DeserializeRow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leaderboard {
    pub shard_id: i32,
    pub user_id: i64,
    pub username: Option<String>,
}

// Response DTOs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardResponse {
    pub rank: i32,
    pub user_id: i64,
    pub username: Option<String>,
    pub score: i64,
}
