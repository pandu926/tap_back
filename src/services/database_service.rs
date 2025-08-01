use sqlx::{PgPool, Row, Acquire};
use sqlx::postgres::PgPoolOptions;

use tracing::info;

use crate::{
    errors::{AppError, Result},
    models::{UserScoreUpdate, TapEventLog},
};

#[derive(Clone)]
pub struct DatabaseService {
    pool: PgPool,
}

impl DatabaseService {
    pub async fn new() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set");



        let pool = PgPoolOptions::new()
            .max_connections(200)                    // Reduced for PgBouncer stability
            .min_connections(50)
            .acquire_timeout(std::time::Duration::from_secs(15))
            .idle_timeout(std::time::Duration::from_secs(900))
            .max_lifetime(std::time::Duration::from_secs(3600)) // Shorter lifetime
            .test_before_acquire(false)             // Disable to avoid prepared statements
            .connect(&database_url)
            .await?;

        // Test connection with simple query - no prepared statements
        let mut conn = pool.acquire().await?;
        let _: (i32,) = sqlx::query_as("SELECT 1 as test")
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database connection test failed: {}", e)))?;

        info!("Database connection pool established via PgBouncer (transaction mode) - prepared statements disabled");
        Ok(Self { pool })
    }

    pub async fn bulk_upsert_scores(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        for u in updates.iter().take(10) {
            info!("User {}: +{} score", u.user_id, u.score_increment);
        }
        info!("...total {} users flushed", updates.len());

        // Separate user_ids and score_increments for UNNEST
        let user_ids: Vec<i64> = updates.iter().map(|u| u.user_id).collect();
        let score_increments: Vec<i64> = updates.iter().map(|u| u.score_increment).collect();

        // Use explicit transaction with fresh connection
        let mut conn = self.pool.acquire().await?;
        let mut tx = conn.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO players (user_id, score, last_seen)
            SELECT
                user_id,
                score_increment,
                NOW()
            FROM
                UNNEST($1::bigint[], $2::bigint[]) AS t(user_id, score_increment)
            ON CONFLICT (user_id) DO UPDATE
            SET
                score = players.score + excluded.score,
                last_seen = NOW()
            "#
        )
        .bind(&user_ids)
        .bind(&score_increments)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn log_tap_event(&self, log: &TapEventLog) -> Result<()> {
        // Get fresh connection for each operation
        let mut conn = self.pool.acquire().await?;
        
        sqlx::query(
            "INSERT INTO tap_events (event_time, user_id, tap_count) VALUES ($1, $2, $3)"
        )
        .bind(log.event_time)
        .bind(log.user_id)
        .bind(log.tap_count)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    pub async fn bulk_copy_logs(&self, logs: &[TapEventLog]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        const BATCH_SIZE: usize = 500; // Smaller batches for PgBouncer
        
        for chunk in logs.chunks(BATCH_SIZE) {
            // Get fresh connection for each batch
            let mut conn = self.pool.acquire().await?;
            let mut tx = conn.begin().await?;
            
            // Collect values for batch insert
            let event_times: Vec<_> = chunk.iter().map(|log| log.event_time).collect();
            let user_ids: Vec<_> = chunk.iter().map(|log| log.user_id).collect();
            let tap_counts: Vec<_> = chunk.iter().map(|log| log.tap_count).collect();
            
            sqlx::query(
                "INSERT INTO tap_events (event_time, user_id, tap_count) SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::integer[])"
            )
            .bind(&event_times)
            .bind(&user_ids)
            .bind(&tap_counts)
            .execute(&mut *tx)
            .await?;
            
            tx.commit().await?;
        }

        info!("Bulk inserted {} tap events in {} batches", logs.len(), (logs.len() + BATCH_SIZE - 1) / BATCH_SIZE);
        Ok(())
    }

    pub async fn get_user_by_id(&self, user_id: i64) -> Result<Option<PlayerRecord>> {
        let mut conn = self.pool.acquire().await?;
        
        let record = sqlx::query(
            "SELECT user_id, score, energy, last_seen FROM players WHERE user_id = $1"
        )
        .bind(user_id)
        .map(|row: sqlx::postgres::PgRow| PlayerRecord {
            user_id: row.get("user_id"),
            score: row.get("score"),
            energy: row.get("energy"),
            last_seen: row.get("last_seen"),
        })
        .fetch_optional(&mut *conn)
        .await?;

        Ok(record)
    }

    pub async fn get_leaderboard(&self, limit: i64, offset: i64) -> Result<Vec<PlayerRecord>> {
        let mut conn = self.pool.acquire().await?;
        
        let records = sqlx::query(
            "SELECT user_id, score, energy, last_seen FROM players ORDER BY score DESC LIMIT $1 OFFSET $2"
        )
        .bind(limit)
        .bind(offset)
        .map(|row: sqlx::postgres::PgRow| PlayerRecord {
            user_id: row.get("user_id"),
            score: row.get("score"),
            energy: row.get("energy"),
            last_seen: row.get("last_seen"),
        })
        .fetch_all(&mut *conn)
        .await?;

        Ok(records)
    }

    pub async fn get_user_rank(&self, user_id: i64) -> Result<Option<i64>> {
        let mut conn = self.pool.acquire().await?;
        
        let result = sqlx::query(
            r#"
            WITH user_score AS (
                SELECT score FROM players WHERE user_id = $1
            )
            SELECT COUNT(*) + 1 as rank
            FROM players p, user_score us
            WHERE p.score > us.score
            "#
        )
        .bind(user_id)
        .map(|row: sqlx::postgres::PgRow| row.get::<Option<i64>, _>("rank"))
        .fetch_optional(&mut *conn)
        .await?;

        Ok(result.flatten())
    }

    pub async fn create_or_update_player(&self, user_id: i64) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        
        sqlx::query(
            "INSERT INTO players (user_id, score, energy, last_seen) VALUES ($1, 0, 1000, NOW()) ON CONFLICT (user_id) DO UPDATE SET last_seen = NOW()"
        )
        .bind(user_id)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }

    pub async fn get_tap_events_for_user(
        &self, 
        user_id: i64, 
        limit: i64, 
        offset: i64
    ) -> Result<Vec<TapEventRecord>> {
        let mut conn = self.pool.acquire().await?;
        
        let records = sqlx::query(
            "SELECT event_time, user_id, tap_count FROM tap_events WHERE user_id = $1 ORDER BY event_time DESC LIMIT $2 OFFSET $3"
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .map(|row: sqlx::postgres::PgRow| TapEventRecord {
            event_time: row.get("event_time"),
            user_id: row.get("user_id"),
            tap_count: row.get("tap_count"),
        })
        .fetch_all(&mut *conn)
        .await?;

        Ok(records)
    }

    pub async fn get_total_taps_last_24h(&self, user_id: i64) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        
        let result = sqlx::query(
            "SELECT COALESCE(SUM(tap_count), 0) as total FROM tap_events WHERE user_id = $1 AND event_time >= NOW() - INTERVAL '24 hours'"
        )
        .bind(user_id)
        .map(|row: sqlx::postgres::PgRow| row.get::<i64, _>("total"))
        .fetch_one(&mut *conn)
        .await?;

        Ok(result)
    }

    pub async fn cleanup_old_events(&self, days_to_keep: i32) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;
        
        let result = sqlx::query(
            "DELETE FROM tap_events WHERE event_time < NOW() - INTERVAL '1 day' * $1"
        )
        .bind(days_to_keep)
        .execute(&mut *conn)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn health_check(&self) -> Result<DatabaseHealth> {
        let start = std::time::Instant::now();
        
        // Use fresh connection for health check
        let mut conn = self.pool.acquire().await?;
        let _: (i32,) = sqlx::query_as("SELECT 1 as health_check")
            .fetch_one(&mut *conn)
            .await?;
            
        let response_time = start.elapsed();

        Ok(DatabaseHealth {
            is_healthy: true,
            response_time_ms: response_time.as_millis() as u64,
            connection_pool_size: 20, // Static value since we can't get it from pool
        })
    }

    // Helper method to execute queries with retry logic for PgBouncer
    async fn execute_with_retry<F, T>(&self, operation: F) -> Result<T>
    where
        F: Fn(&mut sqlx::PgConnection) -> futures::future::BoxFuture<'_, sqlx::Result<T>>,
        T: Send + 'static,
    {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;
        
        for attempt in 1..=MAX_RETRIES {
            let mut conn = match self.pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    last_error = Some(e.into());
                    if attempt < MAX_RETRIES {
                        let backoff = 100 * attempt;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff.into())).await;

                        continue;
                    }
                    break;
                }
            };
            
            match operation(&mut *conn).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e.into());
                    if attempt < MAX_RETRIES {
                        let backoff = 100 * attempt;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff.into())).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| AppError::Internal(anyhow::anyhow!("All retry attempts failed"))))
    }
}

#[derive(Debug)]
pub struct PlayerRecord {
    pub user_id: i64,
    pub score: i64,
    pub energy: i32,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub struct TapEventRecord {
    pub event_time: chrono::DateTime<chrono::Utc>,
    pub user_id: i64,
    pub tap_count: i32,
}

#[derive(Debug, serde::Serialize)]
pub struct DatabaseHealth {
    pub is_healthy: bool,
    pub response_time_ms: u64,
    pub connection_pool_size: u32,
}