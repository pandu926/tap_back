use deadpool_redis::{Config, Pool, Runtime};
use redis::{AsyncCommands, cmd};
use serde_json;
use tracing::{error, info};

use crate::{
    errors::{AppError, Result},
    models::UserScoreUpdate,
};

#[derive(Clone)]
pub struct RedisService {
    pool: Pool,
}

#[derive(Debug)]
pub struct UserStats {
    pub score: i64,
    pub energy: i32,
}

impl RedisService {
    pub async fn new() -> Result<Self> {
        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
            
        let cfg = Config::from_url(redis_url);
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        
        Ok(Self { pool })
    }

    pub async fn process_tap_batch_optimized(&self, batch: Vec<(i64, i64)>) -> Result<()> {
        let mut conn = self.pool.get().await?;
        
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for (user_id, tap_count) in batch {
            pipe.hincr(format!("user:{}", user_id), "score", tap_count)
                .sadd("dirty_users", user_id);
        }
        
        pipe.ignore()
            .query_async(&mut *conn)
            .await?;
            
        Ok(())
    }

    pub async fn get_and_clear_dirty_users(&self) -> Result<Vec<i64>> {
        let mut conn = self.pool.get().await?;

        let mut pipe = redis::pipe();
        pipe.atomic()
            .smembers("dirty_users")
            .del("dirty_users");

        let (dirty_users, _): (Vec<i64>, ()) = pipe.query_async(&mut *conn).await?;
        Ok(dirty_users)
    }

    pub async fn get_user_score(&self, user_id: i64) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        
        let score: Option<i64> = conn
            .hget(format!("user:{}", user_id), "score")
            .await?;
            
        Ok(score.unwrap_or(0))
    }

    pub async fn clear_user_score(&self, user_id: i64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        
        let _: () = conn
            .hdel(format!("user:{}", user_id), "score")
            .await?;
            
        Ok(())
    }

    pub async fn move_to_dlq(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        let mut conn = self.pool.get().await?;

        for update in updates {
            let serialized = serde_json::to_string(update)?;
            let _: () = conn
                .lpush("db_writes_dlq", serialized)
                .await?;
        }

        error!("Moved {} updates to DLQ", updates.len());
        Ok(())
    }

    pub async fn get_dlq_size(&self) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let size: usize = conn.llen("db_writes_dlq").await?;
        Ok(size)
    }

    pub async fn process_dlq_batch(&self, batch_size: usize) -> Result<Vec<UserScoreUpdate>> {
        let mut conn = self.pool.get().await?;

        let mut updates = Vec::new();
        
        for _ in 0..batch_size {
            let item: Option<String> = conn.rpop("db_writes_dlq", None).await?;
            if let Some(serialized) = item {
                match serde_json::from_str::<UserScoreUpdate>(&serialized) {
                    Ok(update) => updates.push(update),
                    Err(e) => {
                        error!("Failed to deserialize DLQ item: {}", e);
                        return Err(AppError::Serialization(e));
                    }
                }
            } else {
                break;
            }
        }

        Ok(updates)
    }

    pub async fn get_user_stats(&self, user_id: i64) -> Result<Option<UserStats>> {
        let mut conn = self.pool.get().await?;
        
        let user_key = format!("user:{}", user_id);
        let (score, energy): (Option<i64>, Option<i32>) = redis::pipe()
            .hget(&user_key, "score")
            .hget(&user_key, "energy")
            .query_async(&mut *conn)
            .await?;

        if score.is_some() || energy.is_some() {
            Ok(Some(UserStats {
                score: score.unwrap_or(0),
                energy: energy.unwrap_or(1000),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn update_user_energy(&self, user_id: i64, energy_delta: i32) -> Result<i32> {
        let mut conn = self.pool.get().await?;
        
        let new_energy: i32 = conn
            .hincr(format!("user:{}", user_id), "energy", energy_delta)
            .await?;
            
        Ok(new_energy)
    }

    pub async fn set_user_last_seen(&self, user_id: i64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let timestamp = chrono::Utc::now().timestamp();
        
        let _: () = conn
            .hset(format!("user:{}", user_id), "last_seen", timestamp)
            .await?;
            
        Ok(())
    }

    pub async fn get_user_last_seen(&self, user_id: i64) -> Result<Option<i64>> {
        let mut conn = self.pool.get().await?;
        
        let timestamp: Option<i64> = conn
            .hget(format!("user:{}", user_id), "last_seen")
            .await?;
            
        Ok(timestamp)
    }
}