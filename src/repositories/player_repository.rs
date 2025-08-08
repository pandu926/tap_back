use crate::database::Database;
use crate::errors::AppError;
use crate::models::player::{CreatePlayerRequest, Player, UpdatePlayerRequest, UserScoreUpdate};

use anyhow::Result;
use chrono::Utc;
use scylla::statement::batch::{Batch, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct PlayerRepository {
    db: Database,
    create_player_stmt: Arc<PreparedStatement>,
    get_player_stmt: Arc<PreparedStatement>,
    update_player_stmt: Arc<PreparedStatement>,
    update_score_stmt: Arc<PreparedStatement>,
    get_all_players_stmt: Arc<PreparedStatement>,
    get_score_stmt: Arc<PreparedStatement>,
    delete_player_stmt: Arc<PreparedStatement>,
    connection_semaphore: Arc<Semaphore>,
    upsert_score_stmt: Arc<PreparedStatement>,
}

impl PlayerRepository {
    pub async fn new(db: Database) -> Result<Self> {
        let create_player_stmt = Arc::new(
    db.session.prepare(
        "INSERT INTO players (user_id, username, first_name, auth_date, level, energy, max_energy, tap_value, energy_recharge_rate, referral_code, referred_by_id, last_seen, score) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ).await?
);

        let upsert_score_stmt = Arc::new(
            db.session
                .prepare("INSERT INTO players (user_id, score, last_seen) VALUES (?, ?, ?)")
                .await
                .map_err(|e| {
                    AppError::Internal(anyhow::anyhow!("Failed to prepare upsert statement: {}", e))
                })?,
        );

        let get_player_stmt = Arc::new(
    db.session.prepare(
        "SELECT user_id, username, first_name, auth_date, level, score, energy, max_energy, energy_last_recharged, tap_value, energy_recharge_rate, referral_code, referred_by_id, last_seen
         FROM players WHERE user_id = ?"
    ).await?
);

        let update_player_stmt = Arc::new(
            db.session.prepare(
                "UPDATE players SET username = ?, first_name = ?, level = ?, energy = ?, max_energy = ?, tap_value = ?, energy_recharge_rate = ?, last_seen = ? 
                WHERE user_id = ?"
            ).await?
        );

        let update_score_stmt = Arc::new(
            db.session
                .prepare("UPDATE players SET score = ?, last_seen = ? WHERE user_id = ?")
                .await?,
        );

        let get_all_players_stmt = Arc::new(
            db.session.prepare(
                "SELECT user_id, username, first_name, auth_date, level, energy, max_energy, energy_last_recharged, tap_value, energy_recharge_rate, referral_code, referred_by_id, last_seen 
                FROM players LIMIT ?"
            ).await?
        );

        let delete_player_stmt = Arc::new(
            db.session
                .prepare("DELETE FROM players WHERE user_id = ?")
                .await?,
        );
        let get_score_stmt = Arc::new(
            db.session
                .prepare("SELECT score FROM players WHERE user_id = ?")
                .await?,
        );
        let connection_semaphore = Arc::new(Semaphore::new(5000)); // Allow 1000 concurrent operations

        Ok(Self {
            db,
            create_player_stmt,
            get_player_stmt,
            update_player_stmt,
            update_score_stmt,
            get_all_players_stmt,
            delete_player_stmt,
            get_score_stmt,
            upsert_score_stmt,
            connection_semaphore,
        })
    }

    pub async fn create(&self, request: CreatePlayerRequest) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(
                &self.create_player_stmt,
                (
                    request.user_id,
                    request.username,
                    request.first_name,
                    now,
                    1i32,    // default level
                    1000i32, // default energy
                    1000i32, // default max_energy
                    1i32,    // default tap_value
                    1i32,    // default energy_recharge_rate
                    request.referral_code,
                    request.referred_by_id,
                    now,
                    0i64,
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn get_user_by_id(&self, user_id: i64) -> Result<Option<Player>> {
        debug!("Executing get_user_by_id for {}", user_id);

        let result = self
            .db
            .session
            .execute_unpaged(&self.get_player_stmt, (user_id,))
            .await
            .map_err(|e| {
                error!("Failed to execute statement: {}", e);
                AppError::Internal(anyhow::anyhow!("Failed to execute statement: {}", e))
            })?;

        debug!("Query executed successfully");

        let rows_result = result.into_rows_result().map_err(|e| {
            error!("Failed to convert result into rows: {}", e);
            AppError::Internal(anyhow::anyhow!("Failed to convert result into rows: {}", e))
        })?;

        debug!("Rows_result created successfully");

        let mut rows = rows_result.rows::<Player>().map_err(|e| {
            error!("Failed to deserialize Player struct: {}", e);
            AppError::Internal(anyhow::anyhow!(
                "Failed to deserialize Player struct: {}",
                e
            ))
        })?;

        debug!("Row deserialization started");

        if let Some(player) = rows.next() {
            let player = player.map_err(|e| {
                error!("Failed to parse Player row: {}", e);
                AppError::Internal(anyhow::anyhow!("Failed to parse Player row: {}", e))
            })?;
            debug!("Player found: {:?}", player);
            Ok(Some(player))
        } else {
            debug!("No player found for user_id: {}", user_id);
            Ok(None)
        }
    }

    pub async fn update(&self, request: UpdatePlayerRequest) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(
                &self.update_player_stmt,
                (
                    request.username,
                    request.first_name,
                    request.level,
                    request.energy,
                    request.max_energy,
                    request.tap_value,
                    request.energy_recharge_rate,
                    now,
                    request.user_id,
                ),
            )
            .await?;

        Ok(())
    }
    pub async fn bulk_upsert_scores(&self, updates: &[UserScoreUpdate]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let now = CqlTimestamp(Utc::now().timestamp_millis());

        // Log sample for monitoring
        for u in updates.iter().take(10) {
            info!("User {}: flushing new score {}", u.user_id, u.new_score);
        }
        info!("...total {} users flushed to ScyllaDB", updates.len());

        // Process in parallel batches for better performance
        const BATCH_SIZE: usize = 100;
        let chunks: Vec<_> = updates.chunks(BATCH_SIZE).collect();

        // Create futures for parallel processing
        let futures: Vec<_> = chunks
            .into_iter()
            .map(|chunk| {
                let session = self.db.session.clone();
                let stmt = self.upsert_score_stmt.clone();
                let semaphore = self.connection_semaphore.clone();
                let now = now.clone();

                async move {
                    let _permit = semaphore.acquire().await.unwrap();

                    // Use batch statement for better performance
                    let mut batch: Batch = Batch::new(BatchType::Logged);

                    for _ in chunk {
                        batch.append_statement((*stmt).clone());
                    }

                    let values: Vec<_> = chunk
                        .iter()
                        .map(|update| (update.user_id, update.new_score, now))
                        .collect();

                    session.batch(&batch, values).await
                }
            })
            .collect();
        info!(
            "🔄 Starting batch upsert to ScyllaDB: {} users",
            updates.len()
        );
        // Execute all batches concurrently
        let results = futures::future::join_all(futures).await;
        info!("✅ Completed ScyllaDB upsert");
        // Check for errors
        for result in results {
            result
                .map_err(|e| AppError::Internal(anyhow::anyhow!("Batch upsert failed: {}", e)))?;
        }

        Ok(())
    }

    pub async fn update_score(&self, user_id: i64, score_increment: i64) -> Result<()> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_score_stmt, (user_id,))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_score_stmt: {}", e))?;

        let rows_result = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to get rows: {}", e))?;

        let mut rows = rows_result
            .rows::<(Option<i64>,)>() // Gunakan Option<i64> untuk handle null
            .map_err(|e| anyhow::anyhow!("Failed to deserialize score row: {}", e))?;

        let (current_score_opt,) = rows
            .next()
            .ok_or_else(|| anyhow::anyhow!("No score row found"))?
            .map_err(|e| anyhow::anyhow!("Failed to parse score row: {}", e))?;

        let current_score = current_score_opt.unwrap_or(0); // fallback ke 0 jika null

        let new_score = current_score + score_increment;
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(&self.update_score_stmt, (new_score, now, user_id))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute update_score_stmt: {}", e))?;

        Ok(())
    }

    pub async fn get_all(&self, limit: i32) -> Result<Vec<Player>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_all_players_stmt, (limit,))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_all_players_stmt: {}", e))?;

        let rows_result = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to get rows from result: {}", e))?;

        let players: Vec<Player> = rows_result
            .rows::<Player>()?
            .map(|row| row.map_err(|e| anyhow::anyhow!("Failed to parse Player row: {}", e)))
            .collect::<std::result::Result<_, _>>()?;

        Ok(players)
    }

    pub async fn delete(&self, user_id: i64) -> Result<()> {
        self.db
            .session
            .execute_unpaged(&self.delete_player_stmt, (user_id,))
            .await?;

        Ok(())
    }
    pub async fn health_check(&self) -> Result<DatabaseHealth> {
        let start = std::time::Instant::now();

        let _result = self
            .db
            .session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| AppError::Internal(anyhow::anyhow!("Health check failed: {}", e)))?;

        let response_time = start.elapsed();

        Ok(DatabaseHealth {
            is_healthy: true,
            response_time_ms: response_time.as_millis() as u64,
            active_connections: 0, // ScyllaDB driver doesn't expose this easily
            idle_connections: 0,
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct DatabaseHealth {
    pub is_healthy: bool,
    pub response_time_ms: u64,
    pub active_connections: u32,
    pub idle_connections: u32,
}
