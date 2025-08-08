use deadpool_redis::{Config, Pool, Runtime};
use redis::{AsyncCommands, Script};
use tracing::{info, warn};

use crate::errors::Result;
use crate::models::player::UserScoreUpdate;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisService {
    pool: Pool,
    tap_script: Script,
    batch_clear_script: Script,
    energy_regen_script: Script,
}

#[derive(Debug)]
pub struct TapResult {
    pub new_score: i64,
    pub new_energy: i32,
}

#[derive(Debug)]
pub struct UserStats {
    pub score: i64,
    pub energy: i32,
}

#[derive(Debug, serde::Serialize)]
pub struct RedisHealth {
    pub is_healthy: bool,
    pub response_time_ms: u64,
    pub pool_size: usize,
    pub available_connections: usize,
}

impl RedisService {
    pub async fn new() -> Result<Self> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        let mut cfg = Config::from_url(redis_url);

        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: std::env::var("REDIS_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50),
            timeouts: deadpool_redis::Timeouts {
                wait: Some(Duration::from_millis(500)),
                create: Some(Duration::from_secs(1)),
                recycle: Some(Duration::from_millis(500)),
            }
            .into(),
            ..Default::default()
        });
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        info!("✅ Redis connection pool created.");

        let tap_script = Script::new(Self::get_optimized_tap_script());
        let batch_clear_script = Script::new(Self::get_batch_clear_script());
        let energy_regen_script = Script::new(Self::get_energy_regen_script());

        Ok(Self {
            pool,
            tap_script,
            batch_clear_script,
            energy_regen_script,
        })
    }

    fn get_optimized_tap_script() -> &'static str {
        r#"
        local user_key = KEYS[1]
        local dirty_set_key = KEYS[2]
        local tap_increment = tonumber(ARGV[1])
        local energy_cost = tonumber(ARGV[2])
        local user_id_arg = ARGV[3]
        local current_time = ARGV[4]
        local user_data = redis.call('HMGET', user_key, 'energy', 'score')
        local current_energy = tonumber(user_data[1]) or 1000
        local current_score = tonumber(user_data[2]) or 0
        if current_energy < energy_cost then
            return {current_score, current_energy, 'NO_ENERGY'}
        end
        local new_score = current_score + tap_increment
        local new_energy = current_energy - energy_cost
        redis.call('HMSET', user_key, 'score', new_score, 'energy', new_energy, 'last_seen', current_time)
        
        -- --- LOGGING DARI DALAM LUA ---
        local sadd_result = redis.call('SADD', dirty_set_key, user_id_arg)
        redis.log(redis.LOG_WARNING, "LUA_DEBUG: SADD executed for user " .. user_id_arg .. " on key " .. dirty_set_key .. ". Result: " .. tostring(sadd_result))
        -- --- AKHIR LOGGING ---

        redis.call('EXPIRE', user_key, 604800)
        return {new_score, new_energy, 'OK'}
    "#
    }

    fn get_batch_clear_script() -> &'static str {
        r#"
            local dirty_set_key = KEYS[1]
            local batch_size = tonumber(ARGV[1])
            local user_ids = redis.call('SPOP', dirty_set_key, batch_size)
            if #user_ids == 0 then return {} end
            local result = {}
            for i, user_id in ipairs(user_ids) do
                local user_key = 'user:' .. user_id
                local score = redis.call('HGET', user_key, 'score')
                if score then
                    table.insert(result, user_id)
                    table.insert(result, score)
                end
            end
            return result
        "#
    }

    fn get_energy_regen_script() -> &'static str {
        r#"
            local user_key = KEYS[1]
            local regen_amount = tonumber(ARGV[1])
            local max_energy = tonumber(ARGV[2])
            local current_energy = tonumber(redis.call('HGET', user_key, 'energy')) or 1000
            local new_energy = math.min(current_energy + regen_amount, max_energy)
            redis.call('HSET', user_key, 'energy', new_energy)
            return new_energy
        "#
    }
    fn get_sync_regen_script() -> &'static str {
        r#"
            local user_key = KEYS[1]
            local current_time = tonumber(ARGV[1])

            local REGEN_INTERVAL_SECONDS = 14400 -- 4 jam
            
            local user_data = redis.call('HMGET', user_key, 'energy', 'score', 'last_seen', 'max_energy', 'energy_recharge_rate')
            
            if not user_data[1] then return redis.error_reply("User not in cache") end

            local current_energy = tonumber(user_data[1])
            local current_score = tonumber(user_data[2])
            local last_seen = tonumber(user_data[3]) or current_time
            local max_energy = tonumber(user_data[4]) or 1000
            local recharge_rate = tonumber(user_data[5]) or 1

            if current_energy < max_energy then
                local time_passed = current_time - last_seen
                if time_passed > 0 then
                    local cycles = math.floor(time_passed / REGEN_INTERVAL_SECONDS)
                    if cycles > 0 then
                        current_energy = math.min(current_energy + cycles * recharge_rate, max_energy)
                    end
                end
            end
            
            redis.call('HMSET', user_key, 'energy', current_energy, 'last_seen', current_time)
            
            return {current_score, current_energy}
        "#
    }

    // Fungsi baru yang akan dipanggil oleh handler /sync
    pub async fn get_user_stats_with_regen(&self, user_id: i64) -> Result<UserStats> {
        let mut conn = self.pool.get().await?;
        let script = Script::new(Self::get_sync_regen_script());

        let (score, energy): (i64, i32) = script
            .key(format!("user:{}", user_id))
            .arg(chrono::Utc::now().timestamp())
            .invoke_async(&mut conn)
            .await?;

        Ok(UserStats { score, energy })
    }
    /// Menulis data pemain dari DB ke Redis, biasanya setelah cache miss.
    pub async fn warm_up_cache(&self, user_id: i64, score: i64, energy: i32) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let user_key = format!("user:{}", user_id);
        redis::pipe()
            .atomic()
            .hset(&user_key, "score", score)
            .hset(&user_key, "energy", energy)
            .hset(&user_key, "last_seen", chrono::Utc::now().timestamp())
            .expire(&user_key, 604800) // 7 hari TTL
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Mengambil dan membersihkan batch skor dari dirty set.
    pub async fn get_and_clear_dirty_scores_batch_resilient(
        &self,
        batch_size: usize,
    ) -> Result<Vec<UserScoreUpdate>> {
        let mut conn = self.pool.get().await?;
        let raw_result: Vec<String> = self
            .batch_clear_script
            .key("dirty_users")
            .arg(batch_size)
            .invoke_async(&mut conn)
            .await?;

        // Cetak hasil mentah ini. Jika ini kosong, berarti masalah ada di skrip Lua.
        tracing::info!("[DEBUG] Raw result from Lua script: {:?}", raw_result);

        let updates: Vec<UserScoreUpdate> = raw_result
            .chunks_exact(2)
            .filter_map(|chunk| {
                let user_id_res = chunk[0].parse::<i64>();
                let total_score_res = chunk[1].parse::<i64>();

                // LOG DETEKTIF #2: Lihat proses parsing untuk setiap data.
                tracing::info!(
                    "[DEBUG] Parsing chunk: user_id={:?}, total_score={:?}",
                    user_id_res,
                    total_score_res
                );

                if let (Ok(user_id), Ok(total_score)) = (user_id_res, total_score_res) {
                    Some(UserScoreUpdate {
                        user_id,
                        new_score: total_score,
                    })
                } else {
                    warn!("Failed to parse user score update chunk: {:?}", chunk);
                    None
                }
            })
            .collect();
        tracing::info!(
            "[DEBUG] Successfully created {} UserScoreUpdate structs",
            updates.len()
        );
        Ok(updates)
    }

    pub async fn get_queue_size(&self) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        Ok(conn.scard("dirty_users").await?)
    }

    pub async fn get_queue_size_dlq(&self) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        Ok(conn.scard("dlq_users").await?)
    }

    /// Memindahkan data yang gagal diproses ke Dead-Letter Queue.
    pub async fn move_to_dlq(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();

        pipe.atomic();

        for update in updates {
            let user_id_str = update.user_id.to_string();
            let dlq_key = format!("dlq:user:{}", user_id_str);
            pipe.hset(&dlq_key, "score", update.new_score)
                .sadd("dlq_users", &user_id_str)
                .expire(&dlq_key, 86400 * 14); // 14 hari
        }
        pipe.query_async::<()>(&mut conn).await?;
        warn!("Moved {} updates to DLQ", updates.len());
        Ok(())
    }

    /// Mengintip beberapa user ID dari DLQ tanpa menghapusnya.
    pub async fn peek_dlq_users(&self, count: usize) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        Ok(conn
            .srandmember_multiple("dlq_users", count as usize)
            .await?)
    }

    /// Mengambil data skor untuk sekumpulan user ID dari DLQ.
    pub async fn get_dlq_user_scores(&self, user_ids: &[String]) -> Result<Vec<UserScoreUpdate>> {
        if user_ids.is_empty() {
            return Ok(vec![]);
        }
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            pipe.hget(format!("dlq:user:{}", user_id), "score");
        }
        let scores: Vec<Option<i64>> = pipe.query_async(&mut conn).await?;
        let updates = user_ids
            .iter()
            .zip(scores)
            .filter_map(|(id_str, score_opt)| {
                if let (Ok(user_id), Some(score)) = (id_str.parse::<i64>(), score_opt) {
                    Some(UserScoreUpdate {
                        user_id,
                        new_score: score,
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(updates)
    }

    /// Menghapus sekumpulan user dari DLQ setelah berhasil diproses.
    pub async fn remove_from_dlq(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.get().await?;
        let mut pipe = redis::pipe();
        for update in updates {
            let user_id_str = update.user_id.to_string();
            pipe.srem("dlq_users", &user_id_str)
                .del(format!("dlq:user:{}", user_id_str));
        }
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    /// Memproses satu tap (atau batch tap yang sudah diagregasi) secara atomik.
    pub async fn process_tap_atomic(
        &self,
        user_id: i64,
        tap_increment: i64,
        energy_cost: i32,
    ) -> Result<TapResult> {
        let mut conn = self.pool.get().await?;
        let (new_score, new_energy, _status): (i64, i32, String) = self
            .tap_script
            .key(format!("user:{}", user_id))
            .key("dirty_users")
            .arg(tap_increment)
            .arg(energy_cost)
            .arg(user_id)
            .arg(chrono::Utc::now().timestamp())
            .invoke_async(&mut conn)
            .await?;
        Ok(TapResult {
            new_score,
            new_energy,
        })
    }

    /// Mengambil statistik dasar pengguna (skor & energi) langsung dari Redis.
    pub async fn get_user_stats(&self, user_id: i64) -> Result<UserStats> {
        let mut conn = self.pool.get().await?;
        let user_key = format!("user:{}", user_id);
        let (score, energy): (Option<i64>, Option<i32>) =
            conn.hmget(&user_key, &["score", "energy"]).await?;

        // Jika key tidak ada sama sekali di cache, kembalikan error.
        // Ini adalah sinyal bagi `handle_sync_session` bahwa terjadi "cache miss".
        if score.is_none() && energy.is_none() {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "User not found in cache",
            ))
            .into());
        }

        Ok(UserStats {
            score: score.unwrap_or(0),
            energy: energy.unwrap_or(1000),
        })
    }

    /// Memeriksa kesehatan koneksi ke Redis.
    pub async fn health_check(&self) -> Result<RedisHealth> {
        let start = std::time::Instant::now();
        let mut conn = self.pool.get().await?;
        let _: () = redis::cmd("PING").query_async(&mut conn).await?;
        let response_time = start.elapsed().as_millis() as u64;
        let pool_status = self.pool.status();

        Ok(RedisHealth {
            is_healthy: true,
            response_time_ms: response_time,
            pool_size: pool_status.size,
            available_connections: pool_status.available,
        })
    }
}
