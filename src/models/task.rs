use scylla::{value::CqlTimestamp, DeserializeRow};
use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, DeserializeRow)]
pub struct Task {
    pub task_type: String,
    pub task_id: Uuid,
    pub title: Option<String>,
    pub description: Option<String>,
    pub completion_target: Option<i32>,
    pub reward_score: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeserializeRow)]
pub struct PlayerTask {
    pub user_id: i64,
    pub task_id: Uuid,
    pub progress: Option<i32>,
    pub completed_at: Option<i64>,
    pub claimed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    pub task_type: String,
    pub title: String,
    pub description: Option<String>,
    pub completion_target: i32,
    pub reward_score: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTaskProgressRequest {
    pub user_id: i64,
    pub task_id: Uuid,
    pub progress: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResponse {
    pub task_id: Uuid,
    pub task_type: String,
    pub title: String,
    pub description: Option<String>,
    pub completion_target: i32,
    pub reward_score: i64,
    pub progress: Option<i32>,
    pub completed: bool,
    pub claimed: bool,
}
