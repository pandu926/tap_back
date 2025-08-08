use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use deadpool;
use scylla::errors::{BadQuery, ExecutionError};
use scylla::response::query_result::RowsError;
use serde_json::json;
use thiserror::Error;
use tokio::time::error::Elapsed;
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Scylla query execution error: {0}")]
    ScyllaExec(#[from] ExecutionError),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Timeout error: {0}")]
    Timeout(#[from] Elapsed),

    #[error("Scylla query execution error: {0}")]
    ScyllaQuery(#[from] BadQuery),

    #[error("Scylla result deserialization error: {0}")]
    ScyllaRows(#[from] RowsError),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Anti-cheat violation: {0}")]
    AntiCheat(String),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("Redis pool error: {0}")]
    RedisPool(#[from] deadpool::managed::PoolError<redis::RedisError>),

    #[error("Redis pool creation error: {0}")]
    RedisCreatePool(#[from] deadpool_redis::CreatePoolError),

    #[error("JWT error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self {
            AppError::Validation(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            AppError::Auth(ref msg) => (StatusCode::UNAUTHORIZED, msg.to_string()),
            AppError::Authentication(_) => (StatusCode::FORBIDDEN, self.to_string()),
            AppError::AntiCheat(_) => (StatusCode::TOO_MANY_REQUESTS, self.to_string()),
            AppError::Timeout(_) => (StatusCode::REQUEST_TIMEOUT, "Request timed out".to_string()),
            AppError::Jwt(e) => (StatusCode::UNAUTHORIZED, format!("Invalid token: {}", e)),

            AppError::Redis(_)
            | AppError::ScyllaExec(_)
            | AppError::ScyllaQuery(_)
            | AppError::ScyllaRows(_)
            | AppError::RedisPool(_)
            | AppError::RedisCreatePool(_)
            | AppError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            ),
            AppError::Serialization(_) => {
                (StatusCode::BAD_REQUEST, "Invalid JSON format".to_string())
            }
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
