use crate::{database::Database, models::leaderboard::LeaderboardResponse};

use anyhow::{Context, Result};

use scylla::statement::prepared::PreparedStatement;

use std::sync::Arc;
#[derive(Clone)]
pub struct LeaderboardRepository {
    db: Database,
    update_leaderboard_stmt: Arc<PreparedStatement>,
    get_leaderboard_stmt: Arc<PreparedStatement>,
    get_score_stmt: Arc<PreparedStatement>,
}

impl LeaderboardRepository {
    pub async fn new(db: Database) -> Result<Self> {
        let update_leaderboard_stmt = Arc::new(
            db.session
                .prepare(
                    "UPDATE leaderboard \
             SET score = ?, username = ? \
             WHERE shard_id = ? AND user_id = ?",
                )
                .await?,
        );

        let get_leaderboard_stmt = Arc::new(
            db.session
                .prepare("SELECT shard_id, user_id, username, score FROM leaderboard WHERE shard_id = ? LIMIT ?")
                .await?
        );

        let get_score_stmt = Arc::new(
            db.session
                .prepare("SELECT score FROM players WHERE user_id = ?")
                .await?,
        );

        Ok(Self {
            db,
            update_leaderboard_stmt,
            get_leaderboard_stmt,
            get_score_stmt,
        })
    }

    pub async fn update_score(
        &self,
        user_id: i64,
        username: Option<String>,
        score_increment: i64,
    ) -> Result<()> {
        let shard_id = 0;
        self.db
            .session
            .execute_unpaged(
                &self.update_leaderboard_stmt,
                (score_increment, username, shard_id, user_id),
            )
            .await
            .context("Failed to execute update_leaderboard_stmt")?;
        Ok(())
    }

    pub async fn get_leaderboard(&self, limit: i32) -> Result<Vec<LeaderboardResponse>> {
        let shard_id = 0;
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_leaderboard_stmt, (shard_id, limit))
            .await
            .context("Failed to execute get_leaderboard_stmt")?;

        let rows = result
            .into_rows_result()
            .context("Failed to extract rows in get_leaderboard")?;

        let mut leaderboard = Vec::new();
        let mut rank = 1;

        for entry_result in rows.rows::<(i32, i64, Option<String>, i64)>()? {
            let (shard_id, user_id, username, score) = entry_result?;

            leaderboard.push(LeaderboardResponse {
                rank,
                user_id,
                username,
                score,
            });

            rank += 1;
        }

        Ok(leaderboard)
    }

    pub async fn get_player_rank(&self, user_id: i64) -> Result<Option<i32>> {
        // Get all scores higher than user's score and count them
        let user_score = self.get_player_score(user_id).await.unwrap_or(0);

        let result = self
            .db
            .session
            .query_unpaged(
                "SELECT COUNT(*) FROM leaderboard WHERE shard_id = 0 AND score > ?",
                (user_score,),
            )
            .await
            .context("Failed to get player rank")?;

        let rank = result
            .into_rows_result()
            .context("Failed to extract rows in get_player_rank")?
            .maybe_first_row::<(i64,)>()
            .context("Failed to parse COUNT(*) in get_player_rank")?
            .map(|(count,)| (count + 1) as i32);

        Ok(rank)
    }

    async fn get_player_score(&self, user_id: i64) -> Result<i64> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_score_stmt, (user_id,))
            .await
            .context("Failed to load player score")?;

        let score = result
            .into_rows_result()
            .context("Failed to extract rows in get_player_score")?
            .maybe_first_row::<(i64,)>()
            .context("Failed to parse score as i64")?
            .map(|(val,)| val)
            .unwrap_or(0);

        Ok(score)
    }
}
