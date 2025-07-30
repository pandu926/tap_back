use sqlx::{PgPool, Row};
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
        .max_connections(100)              // Naikkan sesuai beban
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&database_url)
        .await?;

    info!("Database connection pool established");
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

        // Use UNNEST with ON CONFLICT for high-performance bulk upsert
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
        .execute(&self.pool)
        .await?;

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

pub async fn bulk_copy_logs(&self, logs: &[TapEventLog]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    let mut conn = self.pool.acquire().await?;

    // Start COPY operation
    let mut copy_in = conn.copy_in_raw(
        "COPY tap_events (event_time, user_id, tap_count) FROM STDIN WITH (FORMAT CSV)"
    ).await?;

    // Serialize data to CSV format in memory
    let csv_data = {
        let mut data = Vec::new();
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut data);

        for log in logs {
            writer.serialize((
                log.event_time.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                log.user_id,
                log.tap_count,
            )).map_err(|e| AppError::Internal(anyhow::anyhow!("CSV serialize error: {}", e)))?;
        }

        writer.flush()
            .map_err(|e| AppError::Internal(anyhow::anyhow!("CSV flush error: {}", e)))?;
        
        // Drop the writer here, then return the data
        drop(writer);
        data
    }; // csv_data now owns the Vec<u8> and writer is dropped

    // Stream CSV data to database
    copy_in.send(csv_data).await?;

    // Finish COPY operation
    copy_in.finish().await?;

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
        let result = sqlx::query!(
            r#"
            SELECT rank FROM (
                SELECT user_id, RANK() OVER (ORDER BY score DESC) as rank
                FROM players
            ) ranked
            WHERE user_id = $1
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
    let query = format!(
        "DELETE FROM tap_events WHERE event_time < NOW() - INTERVAL '{} days'",
        days_to_keep
    );
    
    let result = sqlx::query(&query)
        .execute(&self.pool)
        .await?;
    
    Ok(result.rows_affected())
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