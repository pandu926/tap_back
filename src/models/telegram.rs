use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramUser {
    pub id: i64,
    pub first_name: String,
    pub last_name: Option<String>,
    pub username: Option<String>,
    pub language_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthRequest {
    #[serde(rename = "initData")]
    pub init_data: String,
    pub user: TelegramUser,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: TelegramUser,
    pub expires_at: usize,
    pub(crate) user_id: i64,
}
