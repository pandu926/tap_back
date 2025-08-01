use sqlx::{PgPool, Row, Acquire};
use sqlx::postgres::PgPoolOptions;
use std::io::Write;
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
            .max_connections(30)                    // Reduced for PgBouncer
            .min_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(3))
            .idle_timeout(std::time::Duration::from_secs(300))
            .max_lifetime(std::time::Duration::from_secs(900))
            .test_before_acquire(true)              // Test connections
            .connect(&database_url)
            .await?;

        // Test connection immediately
        sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Database connection test failed: {}", e)))?;

        info!("Database connection pool established via PgBouncer with {} max connections", 30);
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

        // Use explicit transaction for PgBouncer compatibility
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
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
            "#,
            &user_ids,
            &score_increments
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn log_tap_event(&self, log: &TapEventLog) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO tap_events (event_time, user_id, tap_count)
            VALUES ($1, $2, $3)
            "#,
            log.event_time,
            log.user_id,
            log.tap_count
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // IMPORTANT: COPY operations don't work well with PgBouncer in transaction mode
    // Use batch INSERT instead
    pub async fn bulk_copy_logs(&self, logs: &[TapEventLog]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        // For PgBouncer compatibility, use batch INSERT instead of COPY
        const BATCH_SIZE: usize = 1000;
        
        for chunk in logs.chunks(BATCH_SIZE) {
            let mut tx = self.pool.begin().await?;
            
            // Collect values for batch insert
            let mut event_times = Vec::new();
            let mut user_ids = Vec::new();
            let mut tap_counts = Vec::new();
            
            for log in chunk {
                event_times.push(log.event_time);
                user_ids.push(log.user_id);
                tap_counts.push(log.tap_count);
            }
            
            sqlx::query!(
                r#"
                INSERT INTO tap_events (event_time, user_id, tap_count)
                SELECT * FROM UNNEST($1::timestamptz[], $2::bigint[], $3::integer[])
                "#,
                &event_times,
                &user_ids,
                &tap_counts
            )
            .execute(&mut *tx)
            .await?;
            
            tx.commit().await?;
        }

        info!("Bulk inserted {} tap events in batches", logs.len());
        Ok(())
    }

    pub async fn get_user_by_id(&self, user_id: i64) -> Result<Option<PlayerRecord>> {
        let record = sqlx::query_as!(
            PlayerRecord,
            r#"
            SELECT user_id, score, energy, last_seen
            FROM players
            WHERE user_id = $1
            "#,
            user_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(record)
    }

    pub async fn get_leaderboard(&self, limit: i64, offset: i64) -> Result<Vec<PlayerRecord>> {
        let records = sqlx::query_as!(
            PlayerRecord,
            r#"
            SELECT user_id, score, energy, last_seen
            FROM players
            ORDER BY score DESC
            LIMIT $1 OFFSET $2
            "#,
            limit,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    pub async fn get_user_rank(&self, user_id: i64) -> Result<Option<i64>> {
        // Use a more efficient approach for ranking with PgBouncer
        let result = sqlx::query!(
            r#"
            WITH user_score AS (
                SELECT score FROM players WHERE user_id = $1
            )
            SELECT COUNT(*) + 1 as rank
            FROM players p, user_score us
            WHERE p.score > us.score
            "#,
            user_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|r| r.rank.unwrap_or(0)))
    }

    pub async fn create_or_update_player(&self, user_id: i64) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO players (user_id, score, energy, last_seen)
            VALUES ($1, 0, 1000, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET last_seen = NOW()
            "#,
            user_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_tap_events_for_user(
        &self, 
        user_id: i64, 
        limit: i64, 
        offset: i64
    ) -> Result<Vec<TapEventRecord>> {
        let records = sqlx::query_as!(
            TapEventRecord,
            r#"
            SELECT event_time, user_id, tap_count
            FROM tap_events
            WHERE user_id = $1
            ORDER BY event_time DESC
            LIMIT $2 OFFSET $3
            "#,
            user_id,
            limit,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    pub async fn get_total_taps_last_24h(&self, user_id: i64) -> Result<i64> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(tap_count), 0) as total
            FROM tap_events
            WHERE user_id = $1 
            AND event_time >= NOW() - INTERVAL '24 hours'
            "#,
            user_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result.total.unwrap_or(0))
    }

    pub async fn cleanup_old_events(&self, days_to_keep: i32) -> Result<u64> {
        // Use parameterized query instead of format! for security
        let result = sqlx::query!(
            r#"
            DELETE FROM tap_events 
            WHERE event_time < NOW() - INTERVAL '1 day' * $1
            "#,
            days_to_keep
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    // Add health check method
    pub async fn health_check(&self) -> Result<DatabaseHealth> {
        let start = std::time::Instant::now();
        
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await?;
            
        let response_time = start.elapsed();
        let pool_state = self.pool.state();

        Ok(DatabaseHealth {
            is_healthy: true,
            response_time_ms: response_time.as_millis() as u64,
            active_connections: pool_state.connections,
            idle_connections: pool_state.idle_connections,
        })
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
    pub active_connections: u32,
    pub idle_connections: u32,
}