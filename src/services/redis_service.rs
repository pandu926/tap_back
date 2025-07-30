use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;
use serde_json;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::{
    errors::{AppError, Result},
    models::UserScoreUpdate,
};

#[derive(Clone)]
pub struct RedisService {
    pool: Pool,
}

impl RedisService {
    pub async fn new() -> Result<Self> {
        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
            
        // Configure connection pool
        let cfg = Config::from_url(redis_url);
        
        let pool = cfg.create_pool(Some(Runtime::Tokio1))
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to create Redis pool: {}", e)))?;

        // Test connection
        let conn = pool.get().await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Failed to get Redis connection: {}", e)))?;
        
        let mut conn = conn;
        let _: String = redis::cmd("PING").query_async(&mut conn).await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Redis PING failed: {}", e)))?;
        
        info!("Redis connection pool established");

        Ok(Self { pool })
    }

    pub async fn process_tap_batch(&self, user_id: i64, tap_count: i64) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;

        // Use pipeline for atomic operations
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hincr(format!("user:{}", user_id), "score", tap_count)
            .sadd("dirty_users", user_id)
            .ignore(); // Ignore return values for pipeline efficiency

        pipe.query_async(&mut conn).await
            .map_err(|e| {
                error!("Redis pipeline failed for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
        
        Ok(())
    }

    pub async fn get_and_clear_dirty_users(&self) -> Result<Vec<i64>> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;

        // Atomically get all dirty users and clear the set
        let mut pipe = redis::pipe();
        pipe.atomic()
            .smembers("dirty_users")
            .del("dirty_users");

        let (dirty_users, _): (Vec<i64>, ()) = pipe.query_async(&mut conn).await
            .map_err(|e| {
                error!("Failed to get dirty users: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
        
        Ok(dirty_users)
    }

    pub async fn get_user_score(&self, user_id: i64) -> Result<i64> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let score: Option<i64> = conn
            .hget(format!("user:{}", user_id), "score")
            .await
            .map_err(|e| {
                error!("Failed to get score for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
            
        Ok(score.unwrap_or(0))
    }

    pub async fn clear_user_score(&self, user_id: i64) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let _: () = conn
            .hdel(format!("user:{}", user_id), "score")
            .await
            .map_err(|e| {
                error!("Failed to clear score for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
            
        Ok(())
    }

    pub async fn move_to_dlq(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;

        for update in updates {
            let serialized = serde_json::to_string(update)
                .map_err(|e| AppError::Internal(anyhow::anyhow!("Serialization failed: {}", e)))?;
            
            let _: () = conn
                .lpush("db_writes_dlq", serialized)
                .await
                .map_err(|e| {
                    error!("Failed to push to DLQ: {}", e);
                    AppError::Internal(anyhow::anyhow!("Redis operation failed"))
                })?;
        }

        error!("Moved {} updates to DLQ", updates.len());
        Ok(())
    }

    pub async fn get_dlq_size(&self) -> Result<usize> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let size: usize = conn.llen("db_writes_dlq").await
            .map_err(|e| {
                error!("Failed to get DLQ size: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
        
        Ok(size)
    }

    pub async fn process_dlq_batch(&self, batch_size: usize) -> Result<Vec<UserScoreUpdate>> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;

        let mut updates = Vec::new();
        
        for _ in 0..batch_size {
            let item: Option<String> = conn.rpop("db_writes_dlq", None).await
                .map_err(|e| {
                    error!("Failed to pop from DLQ: {}", e);
                    AppError::Internal(anyhow::anyhow!("Redis operation failed"))
                })?;
            
            if let Some(serialized) = item {
                match serde_json::from_str::<UserScoreUpdate>(&serialized) {
                    Ok(update) => updates.push(update),
                    Err(e) => {
                        warn!("Failed to deserialize DLQ item: {}", e);
                        // Continue processing other items
                    }
                }
            } else {
                break; // No more items in DLQ
            }
        }

        Ok(updates)
    }

    pub async fn get_user_stats(&self, user_id: i64) -> Result<Option<UserStats>> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let user_key = format!("user:{}", user_id);
        let (score, energy): (Option<i64>, Option<i32>) = redis::pipe()
            .hget(&user_key, "score")
            .hget(&user_key, "energy")
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                error!("Failed to get user stats for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;

        if score.is_some() {
            Ok(Some(UserStats {
                score: score.unwrap_or(0),
                energy: energy.unwrap_or(1000),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn update_user_energy(&self, user_id: i64, energy_delta: i32) -> Result<i32> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let new_energy: i32 = conn
            .hincr(format!("user:{}", user_id), "energy", energy_delta)
            .await
            .map_err(|e| {
                error!("Failed to update energy for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
            
        Ok(new_energy)
    }

    pub async fn set_user_last_seen(&self, user_id: i64) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Failed to get Redis connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis connection failed"))
            })?;

        let mut conn = conn;
        
        let timestamp = chrono::Utc::now().timestamp();
        
        let _: () = conn
            .hset(format!("user:{}", user_id), "last_seen", timestamp)
            .await
            .map_err(|e| {
                error!("Failed to set last seen for user {}: {}", user_id, e);
                AppError::Internal(anyhow::anyhow!("Redis operation failed"))
            })?;
            
        Ok(())
    }

    // Helper method to get pool status for monitoring
    pub fn get_pool_status(&self) -> PoolStatus {
        let status = self.pool.status();
        PoolStatus {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
        }
    }

    // Method for health check
    pub async fn health_check(&self) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| {
                error!("Health check failed - cannot get connection: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis health check failed"))
            })?;

        let mut conn = conn;
        
        let _: String = redis::cmd("PING").query_async(&mut conn).await
            .map_err(|e| {
                error!("Health check failed - PING failed: {}", e);
                AppError::Internal(anyhow::anyhow!("Redis health check failed"))
            })?;
        
        Ok(())
    }
}

#[derive(Debug)]
pub struct UserStats {
    pub score: i64,
    pub energy: i32,
}

#[derive(Debug)]
pub struct PoolStatus {
    pub max_size: usize,
    pub size: usize,
    pub available: usize,
    pub waiting: usize,
}