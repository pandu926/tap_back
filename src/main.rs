use axum::{routing::post, Router};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tower_http::cors::CorsLayer;
use tracing::{info, error};

mod middleware;
mod routes;
mod services;
mod models;
mod errors;
use tracing_subscriber::fmt::init;

use services::{RedisService, DatabaseService};

#[derive(Clone)]
pub struct AppState {
    pub redis_service: RedisService,
    pub database_service: DatabaseService,
    pub bot_token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    init();

    
    // Load environment variables
    dotenvy::dotenv().ok();
    
    // Initialize services
    let redis_service = RedisService::new().await?;
    let database_service = DatabaseService::new().await?;
    let bot_token = std::env::var("BOT_TOKEN").expect("BOT_TOKEN must be set");
    
    // Create app state
    let app_state = AppState {
        redis_service: redis_service.clone(),
        database_service: database_service.clone(),
        bot_token,
    };
    
    // Start background worker
    let worker_state = app_state.clone();
    tokio::spawn(async move {
        background_worker(worker_state).await;
    });
    
    // Create router
    let app = Router::new()
        .route("/api/v1/tap", post(routes::handle_tap_batch))
        .layer(axum::middleware::from_fn_with_state(
            app_state.clone(),
            middleware::dummy_user_id_auth
        ))
        .layer(CorsLayer::permissive())
        .with_state(app_state);
    
    // Start server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    info!("Server starting on http://0.0.0.0:3001");
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

async fn background_worker(state: AppState) {
    let mut interval = interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        
        match flush_dirty_users(&state).await {
            Ok(count) => {
                if count > 0 {
                    info!("Flushed {} dirty users to database", count);
                }
            }
            Err(e) => {
                error!("Failed to flush dirty users: {}", e);
            }
        }
    }
}

async fn flush_dirty_users(state: &AppState) -> anyhow::Result<usize> {
    // Get dirty users from Redis
    let dirty_users = state.redis_service.get_and_clear_dirty_users().await?;
    
    if dirty_users.is_empty() {
        return Ok(0);
    }
    
    // Get user scores from Redis
    let mut updates = Vec::new();
    for user_id in &dirty_users {
        if let Ok(score) = state.redis_service.get_user_score(*user_id).await {
            updates.push(models::UserScoreUpdate {
                user_id: *user_id,
                score_increment: score,
            });
        }
    }
    
    // Attempt to write to database with retry logic
    let mut retry_count = 0;
    const MAX_RETRIES: u32 = 3;
    
    while retry_count < MAX_RETRIES {
        match state.database_service.bulk_upsert_scores(&updates).await {
            Ok(_) => {
                // Success - clear the scores from Redis
                for user_id in &dirty_users {
                    let _ = state.redis_service.clear_user_score(*user_id).await;
                }
                return Ok(updates.len());
            }
            Err(e) => {
                retry_count += 1;
                error!("Database write failed (attempt {}): {}", retry_count, e);
                
                if retry_count >= MAX_RETRIES {
                    // Move to dead letter queue
                    state.redis_service.move_to_dlq(&updates).await?;
                    error!("Moved {} updates to DLQ after {} retries", updates.len(), MAX_RETRIES);
                    return Ok(0);
                }
                
                // Exponential backoff
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count))).await;
            }
        }
    }
    
    Ok(0)
}