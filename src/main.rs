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

use services::{RedisService, DatabaseService};

#[derive(Clone)]
pub struct AppState {
    pub redis_service: RedisService,
    pub database_service: DatabaseService,
    pub bot_token: String,
    
}

// Custom runtime untuk optimisasi CPU usage
fn main() -> anyhow::Result<()> {
    // Custom runtime dengan thread pool yang lebih kecil
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)              // Sisakan 2 core untuk system
        .thread_stack_size(2 * 1024 * 1024) // 2MB stack (default 8MB)
        .thread_keep_alive(Duration::from_secs(60)) // Keep threads alive longer
        .enable_all()
        .build()?;
        
    runtime.block_on(async {
        // Initialize tracing dengan level WARN
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_target(false) // Disable target untuk reduce overhead
            .compact() // Use compact format untuk less CPU
            .init();

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
        
        // Start background worker dengan priority rendah
        let worker_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.spawn(async move {
                background_worker(worker_state).await;
            });
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
        
        // Start server dengan optimized settings
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
        
        info!("Server starting on http://0.0.0.0:3001");
        
        // Serve with graceful shutdown
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        
        Ok::<(), anyhow::Error>(())
    })?;
    
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
}

// Background worker yang lebih efisien dengan CPU optimization
async fn background_worker(state: AppState) {
    let mut interval = interval(Duration::from_secs(10)); // 10s untuk reduce CPU
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    // Adaptive delay untuk reduce CPU ketika tidak ada kerja
    let mut consecutive_empty_runs = 0;
    const MAX_EMPTY_RUNS: u32 = 6; // 60 detik idle sebelum slow down
    
    loop {
        // Yield untuk memberikan CPU ke tasks lain
        tokio::task::yield_now().await;
        
        interval.tick().await;
        
        // Process dengan timeout untuk avoid blocking
        let flush_future = flush_dirty_users(&state);
        
        match tokio::time::timeout(Duration::from_secs(8), flush_future).await {
            Ok(Ok(count)) => {
                if count > 0 {
                    tracing::warn!("Flushed {} dirty users", count);
                    consecutive_empty_runs = 0; // Reset counter
                } else {
                    consecutive_empty_runs += 1;
                    
                    // Adaptive sleep untuk reduce CPU usage ketika idle
                    if consecutive_empty_runs >= MAX_EMPTY_RUNS {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::error!("Flush error: {}", e);
                consecutive_empty_runs = 0;
            }
            Err(_) => {
                tracing::error!("Flush timeout after 8s");
                consecutive_empty_runs = 0;
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