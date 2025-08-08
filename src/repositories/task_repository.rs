use crate::{
    database::Database,
    models::task::{CreateTaskRequest, PlayerTask, Task, UpdateTaskProgressRequest},
};

use anyhow::{Context, Result};
use chrono::Utc;
use scylla::{statement::prepared::PreparedStatement, value::CqlTimestamp};
use std::sync::Arc;
use uuid::Uuid;
#[derive(Clone)]
pub struct TaskRepository {
    db: Database,
    create_task_stmt: Arc<PreparedStatement>,
    get_task_stmt: Arc<PreparedStatement>,
    get_tasks_by_type_stmt: Arc<PreparedStatement>,
    update_task_stmt: Arc<PreparedStatement>,
    delete_task_stmt: Arc<PreparedStatement>,
    upsert_player_task_stmt: Arc<PreparedStatement>,
    get_player_task_stmt: Arc<PreparedStatement>,
    get_player_tasks_stmt: Arc<PreparedStatement>,
    complete_task_stmt: Arc<PreparedStatement>,
    claim_task_stmt: Arc<PreparedStatement>,
    insert_if_not_exists_stmt: Arc<PreparedStatement>,
}

impl TaskRepository {
    pub async fn new(db: Database) -> Result<Self> {
        let create_task_stmt = Arc::new(
            db.session.prepare(
                "INSERT INTO tasks (task_type, task_id, title, description, completion_target, reward_score) 
                VALUES (?, ?, ?, ?, ?, ?)"
            ).await?
        );

        let get_task_stmt = Arc::new(
            db.session.prepare(
                "SELECT task_type, task_id, title, description, completion_target, reward_score 
                FROM tasks WHERE task_type = ? AND task_id = ?"
            ).await?
        );

        let get_tasks_by_type_stmt = Arc::new(
            db.session.prepare(
                "SELECT task_type, task_id, title, description, completion_target, reward_score 
                FROM tasks WHERE task_type = ?"
            ).await?
        );

        let update_task_stmt = Arc::new(
            db.session.prepare(
                "UPDATE tasks SET title = ?, description = ?, completion_target = ?, reward_score = ? 
                WHERE task_type = ? AND task_id = ?"
            ).await?
        );

        let delete_task_stmt = Arc::new(
            db.session
                .prepare("DELETE FROM tasks WHERE task_type = ? AND task_id = ?")
                .await?,
        );

        let upsert_player_task_stmt = Arc::new(
            db.session
                .prepare("UPDATE player_tasks SET progress = ? WHERE user_id = ? AND task_id = ?")
                .await?,
        );

        let get_player_task_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT user_id, task_id, progress, completed_at, claimed_at 
                FROM player_tasks WHERE user_id = ? AND task_id = ?",
                )
                .await?,
        );

        let get_player_tasks_stmt = Arc::new(
            db.session
                .prepare(
                    "SELECT user_id, task_id, progress, completed_at, claimed_at 
                FROM player_tasks WHERE user_id = ?",
                )
                .await?,
        );

        let complete_task_stmt = Arc::new(
            db.session
                .prepare(
                    "UPDATE player_tasks SET completed_at = ? WHERE user_id = ? AND task_id = ?",
                )
                .await?,
        );

        let claim_task_stmt = Arc::new(
            db.session
                .prepare("UPDATE player_tasks SET claimed_at = ? WHERE user_id = ? AND task_id = ?")
                .await?,
        );
        let insert_if_not_exists_stmt = Arc::new(
            db.session.prepare(
                "INSERT INTO player_tasks (user_id, task_id, progress) VALUES (?, ?, ?) IF NOT EXISTS"
            ).await?
        );

        Ok(Self {
            db,
            create_task_stmt,
            get_task_stmt,
            get_tasks_by_type_stmt,
            update_task_stmt,
            delete_task_stmt,
            upsert_player_task_stmt,
            get_player_task_stmt,
            get_player_tasks_stmt,
            complete_task_stmt,
            claim_task_stmt,
            insert_if_not_exists_stmt,
        })
    }

    pub async fn create_task(&self, request: CreateTaskRequest) -> Result<Uuid> {
        let task_id = Uuid::new_v4();

        self.db
            .session
            .execute_unpaged(
                &self.create_task_stmt,
                (
                    &request.task_type,
                    task_id,
                    &request.title,
                    request.description.as_ref(),
                    request.completion_target,
                    request.reward_score,
                ),
            )
            .await?;

        Ok(task_id)
    }

    pub async fn get_task(&self, task_type: &str, task_id: Uuid) -> Result<Option<Task>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_task_stmt, (task_type, task_id))
            .await
            .context("Failed to execute get_task_stmt")?;

        let task = result
            .into_rows_result()
            .context("Failed to extract rows")?
            .maybe_first_row::<Task>()
            .context("Failed to parse Task")?;

        Ok(task)
    }

    pub async fn get_tasks_by_type(&self, task_type: &str) -> Result<Vec<Task>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_tasks_by_type_stmt, (task_type,))
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to execute get_tasks_by_type_stmt: {}", task_type)
            })?;

        let rows = result.into_rows_result().context(format!(
            "Failed to extract rows for task_type = {}",
            task_type
        ))?;

        let tasks = rows
            .rows::<Task>()?
            .map(|row| {
                row.map_err(|e| {
                    anyhow::anyhow!("Failed to parse Task (task_type = {}): {}", task_type, e,)
                })
            })
            .collect::<std::result::Result<Vec<Task>, _>>()?;

        Ok(tasks)
    }

    pub async fn update_task(
        &self,
        task_type: &str,
        task_id: Uuid,
        title: &str,
        description: Option<&str>,
        completion_target: i32,
        reward_score: i64,
    ) -> Result<()> {
        self.db
            .session
            .execute_unpaged(
                &self.update_task_stmt,
                (
                    title,
                    description,
                    completion_target,
                    reward_score,
                    task_type,
                    task_id,
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn delete_task(&self, task_type: &str, task_id: Uuid) -> Result<()> {
        self.db
            .session
            .execute_unpaged(&self.delete_task_stmt, (task_type, task_id))
            .await?;

        Ok(())
    }

    pub async fn update_task_progress(&self, request: UpdateTaskProgressRequest) -> Result<()> {
        // Insert initial progress if not exists
        self.db
            .session
            .execute_unpaged(
                &self.insert_if_not_exists_stmt,
                (request.user_id, request.task_id, 0i32),
            )
            .await
            .context("Failed to insert initial player_tasks row")?;

        self.db
            .session
            .execute_unpaged(
                &self.upsert_player_task_stmt,
                (request.progress, request.user_id, request.task_id),
            )
            .await
            .context("Failed to update player_tasks.progress")?;

        // Then update the progress (upsert style)
        self.db
            .session
            .execute_unpaged(
                &self.upsert_player_task_stmt,
                (request.progress, request.user_id, request.task_id),
            )
            .await
            .context("Failed to update player_tasks progress")?;

        Ok(())
    }

    pub async fn get_player_task(&self, user_id: i64, task_id: Uuid) -> Result<Option<PlayerTask>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_player_task_stmt, (user_id, task_id))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_player_task_stmt: {}", e))?;

        let task = result
            .into_rows_result()
            .map_err(|e| anyhow::anyhow!("Failed to extract rows: {}", e))?
            .maybe_first_row::<PlayerTask>()
            .map_err(|e| anyhow::anyhow!("Failed to parse PlayerTask: {}", e))?;

        Ok(task)
    }

    pub async fn get_player_tasks(&self, user_id: i64) -> Result<Vec<PlayerTask>> {
        let result = self
            .db
            .session
            .execute_unpaged(&self.get_player_tasks_stmt, (user_id,))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute get_player_tasks_stmt: {}", e))?;

        let rows = result.into_rows_result()?;

        let tasks = rows
            .rows::<PlayerTask>()?
            .map(|row| {
                row.map_err(|e| anyhow::anyhow!("Failed to deserialize PlayerTask row: {}", e))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(tasks)
    }

    pub async fn complete_task(&self, user_id: i64, task_id: Uuid) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(&self.complete_task_stmt, (now, user_id, task_id))
            .await?;

        Ok(())
    }

    pub async fn claim_task_reward(&self, user_id: i64, task_id: Uuid) -> Result<()> {
        let now = CqlTimestamp(Utc::now().timestamp_millis());

        self.db
            .session
            .execute_unpaged(&self.claim_task_stmt, (now, user_id, task_id))
            .await?;

        Ok(())
    }
}
