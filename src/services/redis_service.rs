use deadpool_redis::{Config, Pool, Runtime};
use redis::{AsyncCommands};
use serde_json;
use tracing::{error, info, warn};
use std::collections::HashMap;

use crate::{
    errors::{ Result},
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
            
        let mut cfg = Config::from_url(redis_url);
        
        // Configure pool settings
        if let Some(pool_config) = &mut cfg.pool {
            pool_config.max_size = 50;
            pool_config.timeouts.wait = Some(std::time::Duration::from_secs(10));
            pool_config.timeouts.create = Some(std::time::Duration::from_secs(5));
            pool_config.timeouts.recycle = Some(std::time::Duration::from_secs(3));
        } else {
            cfg.pool = Some(deadpool_redis::PoolConfig {
                max_size: 50,
                timeouts: deadpool_redis::Timeouts {
                    wait: Some(std::time::Duration::from_secs(10)),
                    create: Some(std::time::Duration::from_secs(5)),
                    recycle: Some(std::time::Duration::from_secs(3)),
                },
                queue_mode: deadpool::managed::QueueMode::Fifo,
            });
        }
        
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        
        // Test connection
        let mut conn = pool.get().await?;
        let _: String = redis::cmd("PING").query_async(&mut *conn).await?;
        info!("Redis connection pool established with 50 max connections");
        
        Ok(Self { pool })
    }

    pub async fn process_tap_batch_optimized(&self, batch: Vec<(i64, i64)>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        
        // Use larger pipeline for better throughput
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        // Group operations by user to reduce pipeline size
        let mut user_totals: HashMap<i64, i64> = HashMap::new();
        for (user_id, tap_count) in batch {
            *user_totals.entry(user_id).or_insert(0) += tap_count;
        }
        
        for (user_id, total_taps) in user_totals {
            pipe.hincr(format!("user:{}", user_id), "score", total_taps)
                .sadd("dirty_users", user_id);
        }
        
        pipe.ignore()
            .query_async::<()>(&mut *conn)
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

    // New optimized method for batch processing
    pub async fn get_dirty_users_batch(&self, batch_size: usize) -> Result<Vec<i64>> {
        let mut conn = self.pool.get().await?;
        
        let mut users = Vec::new();
        
        // Use SPOP for atomic get-and-remove operations
        for _ in 0..batch_size {
            let user: Option<i64> = conn.spop("dirty_users").await?;
            match user {
                Some(u) => users.push(u),
                None => break, // No more dirty users
            }
        }
        
        Ok(users)
    }

    // New method to estimate workload without expensive operations
    pub async fn estimate_dirty_count(&self) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let count: usize = conn.scard("dirty_users").await?;
        Ok(count)
    }

    pub async fn get_user_score(&self, user_id: i64) -> Result<i64> {
        let mut conn = self.pool.get().await?;
        
        let score: Option<i64> = conn
            .hget(format!("user:{}", user_id), "score")
            .await?;
            
        Ok(score.unwrap_or(0))
    }

    // New batch method for getting multiple user scores
    pub async fn get_scores_batch(&self, user_ids: &[i64]) -> Result<Vec<UserScoreUpdate>> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        
        // Build pipeline for multiple users
        for &user_id in user_ids {
            pipe.hget(format!("user:{}", user_id), "score");
        }
        
        let scores: Vec<Option<i64>> = pipe.query_async(&mut *conn).await?;
        
        let updates = user_ids
            .iter()
            .zip(scores.iter())
            .filter_map(|(&user_id, &score)| {
                score.filter(|&s| s > 0).map(|s| UserScoreUpdate {
                    user_id,
                    score_increment: s,
                })
            })
            .collect();
            
        Ok(updates)
    }

    pub async fn clear_user_score(&self, user_id: i64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        
        let _: () = conn
            .hdel(format!("user:{}", user_id), "score")
            .await?;
            
        Ok(())
    }

    // New batch method for clearing multiple user scores
    pub async fn clear_scores_batch(&self, user_ids: &[i64]) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for &user_id in user_ids {
            pipe.hdel(format!("user:{}", user_id), "score");
        }
        
        pipe.ignore().query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    pub async fn move_to_dlq(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Batch serialize and push to DLQ
        for update in updates {
            match serde_json::to_string(update) {
                Ok(serialized) => {
                    pipe.lpush("db_writes_dlq", serialized);
                }
                Err(e) => {
                    error!("Failed to serialize update for DLQ: {}", e);
                    continue;
                }
            }
        }

        // Add timestamp for DLQ monitoring
        let timestamp = chrono::Utc::now().timestamp();
        pipe.hset("dlq_stats", "last_write", timestamp)
            .hincr("dlq_stats", "total_writes", updates.len() as i64);

        pipe.ignore().query_async::<()>(&mut *conn).await?;
        warn!("Moved {} updates to DLQ", updates.len());
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
        
        // Use pipeline for batch operations  
        let mut items = Vec::new();
        
        for _ in 0..batch_size {
            let item: Option<String> = conn.rpop("db_writes_dlq", Some(std::num::NonZeroUsize::new(1).unwrap())).await?;
            if let Some(serialized) = item {
                items.push(serialized);
            } else {
                break;
            }
        }
        
        for item in items {
            match serde_json::from_str::<UserScoreUpdate>(&item) {
                Ok(update) => updates.push(update),
                Err(e) => {
                    error!("Failed to deserialize DLQ item: {}", e);
                    // Log corrupted item for debugging
                    warn!("Corrupted DLQ item: {}", item);
                }
            }
        }

        Ok(updates)
    }

    pub async fn get_user_stats(&self, user_id: i64) -> Result<Option<UserStats>> {
        let mut conn = self.pool.get().await?;
        
        let user_key = format!("user:{}", user_id);
        let mut pipe = redis::pipe();
        pipe.hget(&user_key, "score")
            .hget(&user_key, "energy")
            .hget(&user_key, "last_seen");

        let (score, energy, last_seen): (Option<i64>, Option<i32>, Option<i64>) = 
            pipe.query_async(&mut *conn).await?;

        if score.is_some() || energy.is_some() || last_seen.is_some() {
            Ok(Some(UserStats {
                score: score.unwrap_or(0),
                energy: energy.unwrap_or(1000),
            }))
        } else {
            Ok(None)
        }
    }

    // New batch method for getting multiple user stats
    pub async fn get_users_stats_batch(&self, user_ids: &[i64]) -> Result<HashMap<i64, UserStats>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        
        for &user_id in user_ids {
            let user_key = format!("user:{}", user_id);
            pipe.hget(&user_key, "score")
                .hget(&user_key, "energy");
        }
        
        let results: Vec<(Option<i64>, Option<i32>)> = pipe.query_async(&mut *conn).await?;
        
        let mut stats_map = HashMap::new();
        for (i, &user_id) in user_ids.iter().enumerate() {
            if let Some((score, energy)) = results.get(i) {
                if score.is_some() || energy.is_some() {
                    stats_map.insert(user_id, UserStats {
                        score: score.unwrap_or(0),
                        energy: energy.unwrap_or(1000),
                    });
                }
            }
        }
        
        Ok(stats_map)
    }

    pub async fn update_user_energy(&self, user_id: i64, energy_delta: i32) -> Result<i32> {
        let mut conn = self.pool.get().await?;
        
        let new_energy: i32 = conn
            .hincr(format!("user:{}", user_id), "energy", energy_delta)
            .await?;
            
        Ok(new_energy)
    }

    // New batch method for updating multiple user energies
    pub async fn update_users_energy_batch(&self, updates: &[(i64, i32)]) -> Result<Vec<i32>> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        
        for &(user_id, energy_delta) in updates {
            pipe.hincr(format!("user:{}", user_id), "energy", energy_delta);
        }
        
        let new_energies: Vec<i32> = pipe.query_async(&mut *conn).await?;
        Ok(new_energies)
    }

    pub async fn set_user_last_seen(&self, user_id: i64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let timestamp = chrono::Utc::now().timestamp();
        
        let _: () = conn
            .hset(format!("user:{}", user_id), "last_seen", timestamp)
            .await?;
            
        Ok(())
    }

    // New batch method for updating multiple user last_seen timestamps
    pub async fn set_users_last_seen_batch(&self, user_ids: &[i64]) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let timestamp = chrono::Utc::now().timestamp();
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for &user_id in user_ids {
            pipe.hset(format!("user:{}", user_id), "last_seen", timestamp);
        }
        
        pipe.ignore().query_async::<()>(&mut *conn).await?;
        Ok(())
    }

    pub async fn get_user_last_seen(&self, user_id: i64) -> Result<Option<i64>> {
        let mut conn = self.pool.get().await?;
        
        let timestamp: Option<i64> = conn
            .hget(format!("user:{}", user_id), "last_seen")
            .await?;
            
        Ok(timestamp)
    }

    // New method for health check and monitoring
    pub async fn health_check(&self) -> Result<RedisHealth> {
        let start = std::time::Instant::now();
        let mut conn = self.pool.get().await?;
        
        let _: String = redis::cmd("PING").query_async(&mut *conn).await?;
        let response_time = start.elapsed();
        
        // Get pool stats
        let pool_status = self.pool.status();
        
        Ok(RedisHealth {
            is_healthy: true,
            response_time_ms: response_time.as_millis() as u64,
            pool_size: pool_status.size,
            available_connections: pool_status.available,
        })
    }

    // New method for cache warming
    pub async fn warm_cache(&self, user_ids: &[i64]) -> Result<()> {
        if user_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        
        // Pre-load frequently accessed user data
        for &user_id in user_ids {
            let user_key = format!("user:{}", user_id);
            pipe.hgetall(&user_key);
        }
        
        // Execute pipeline but ignore results (just warming cache)
        let _: Vec<std::collections::HashMap<String, String>> = 
            pipe.query_async(&mut *conn).await?;
        
        info!("Warmed cache for {} users", user_ids.len());
        Ok(())
    }

    // New method for cleanup operations
    pub async fn cleanup_expired_users(&self, days_old: i64) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let cutoff_timestamp = chrono::Utc::now().timestamp() - (days_old * 24 * 60 * 60);
        
        // Use Lua script for atomic cleanup operation
        let script = redis::Script::new(r#"
            local cutoff = tonumber(ARGV[1])
            local deleted = 0
            local cursor = "0"
            
            repeat
                local result = redis.call("SCAN", cursor, "MATCH", "user:*", "COUNT", "100")
                cursor = result[1]
                local keys = result[2]
                
                for i = 1, #keys do
                    local last_seen = redis.call("HGET", keys[i], "last_seen")
                    if last_seen and tonumber(last_seen) < cutoff then
                        redis.call("DEL", keys[i])
                        deleted = deleted + 1
                    end
                end
            until cursor == "0"
            
            return deleted
        "#);
        
        let deleted: usize = script.arg(cutoff_timestamp).invoke_async(&mut *conn).await?;
        info!("Cleaned up {} expired user records", deleted);
        
        Ok(deleted)
    }
}

#[derive(Debug, serde::Serialize)]
pub struct RedisHealth {
    pub is_healthy: bool,
    pub response_time_ms: u64,
    pub pool_size: usize,
    pub available_connections: usize,
}