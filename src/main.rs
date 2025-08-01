use axum::{routing::post, Router};
use tokio::time::{ Duration};
use tower_http::cors::CorsLayer;
use tracing::{info, error, warn};

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

fn main() -> anyhow::Result<()> {
    // Optimized runtime configuration
  

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .thread_stack_size(4 * 1024 * 1024) // 4MB stack untuk complex operations
        .thread_keep_alive(Duration::from_secs(300)) // 5 menit keep-alive
        .thread_name("tap-game-worker")
        .enable_all()
        .build()?;
        
    runtime.block_on(async {
        // Optimized tracing configuration
        #[cfg(debug_assertions)]
        let max_level = tracing::Level::INFO;
        #[cfg(not(debug_assertions))]
        let max_level = tracing::Level::WARN;
        
        tracing_subscriber::fmt()
            .with_max_level(max_level)
            .with_target(false)
            .compact()
            .init();

        dotenvy::dotenv().ok();
        
        // Parallel service initialization
        let (redis_service, database_service) = tokio::try_join!(
            RedisService::new(),
            DatabaseService::new()
        )?;
        
        let bot_token = std::env::var("BOT_TOKEN").expect("BOT_TOKEN must be set");
        
        let app_state = AppState {
            redis_service,
            database_service,
            bot_token,
        };
        
        // Start optimized background worker
        let worker_state = app_state.clone();
        tokio::spawn(async move {
            background_worker(worker_state).await;
        });
        
        // Create router with optimized middleware order
        let app = Router::new()
            .route("/api/v1/tap", post(routes::handle_tap_batch))
            .layer(axum::middleware::from_fn_with_state(
                app_state.clone(),
                middleware::validate_init_data
            ))
            .layer(CorsLayer::permissive())
            .with_state(app_state);
        
        // Optimized TCP listener
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
        
        // Set socket options for better performance
        if let Ok(socket) = listener.local_addr() {
            info!("Server starting on http://{}", socket);
        }
        
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        
        Ok::<(), anyhow::Error>(())
    })?;
    
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    
    info!("Graceful shutdown initiated");
}

async fn background_worker(state: AppState) {
    let base_interval = Duration::from_secs(5);
    let mut current_interval = base_interval;
    let mut consecutive_empty = 0;
    const MAX_EMPTY_RUNS: u32 = 6;
    
    info!("Background worker started with adaptive scheduling");
    
    loop {
        tokio::time::sleep(current_interval).await;
        
        // Estimate workload before processing
        let estimated_dirty = match state.redis_service.estimate_dirty_count().await {
            Ok(count) => count,
            Err(e) => {
                warn!("Failed to estimate dirty count: {}", e);
                0
            }
        };
        
        // Skip processing if no work detected
        if estimated_dirty == 0 {
            consecutive_empty += 1;
            current_interval = std::cmp::min(
                Duration::from_secs(30),
                base_interval * 2_u32.pow(consecutive_empty.min(3))
            );
            continue;
        }
        
        // Dynamic batch size based on workload
        let batch_size = match estimated_dirty {
            0..=50 => 25,
            51..=200 => 100,
            201..=500 => 250,
            _ => 500,
        };
        
        let flush_future = flush_dirty_users(&state, batch_size);
        let timeout_duration = Duration::from_secs(std::cmp::max(10, (estimated_dirty / 50) as u64));
        
        match tokio::time::timeout(timeout_duration, flush_future).await {
            Ok(Ok(count)) => {
                if count > 0 {
                    info!("Flushed {} dirty users in batch", count);
                    consecutive_empty = 0;
                    current_interval = base_interval; // Reset to aggressive
                } else {
                    consecutive_empty += 1;
                }
            }
            Ok(Err(e)) => {
                error!("Flush operation failed: {}", e);
                consecutive_empty = 0;
                current_interval = Duration::from_secs(15); // Moderate retry interval
            }
            Err(_) => {
                error!("Flush operation timed out after {:?}", timeout_duration);
                consecutive_empty = 0;
                current_interval = Duration::from_secs(10);
            }
        }
        
        // Adaptive interval adjustment
        if consecutive_empty >= MAX_EMPTY_RUNS {
            current_interval = Duration::from_secs(30); // Slow down when consistently idle
        }
    }
}

async fn flush_dirty_users(state: &AppState, batch_size: usize) -> anyhow::Result<usize> {
    // Get dirty users in controlled batches
    let dirty_users = state.redis_service.get_dirty_users_batch(batch_size).await?;
    
    if dirty_users.is_empty() {
        return Ok(0);
    }
    
    // Parallel score fetching with chunking
    let chunk_size = std::cmp::min(50, dirty_users.len());
    let score_futures: Vec<_> = dirty_users
        .chunks(chunk_size)
        .map(|chunk| {
            let redis_service = state.redis_service.clone();
            let chunk_vec = chunk.to_vec();
            tokio::spawn(async move {
                redis_service.get_scores_batch(&chunk_vec).await
            })
        })
        .collect();
    
    // Collect results with error handling
    let mut all_updates = Vec::new();
    for future in score_futures {
        match future.await {
            Ok(Ok(updates)) => all_updates.extend(updates),
            Ok(Err(e)) => warn!("Batch score fetch failed: {}", e),
            Err(e) => warn!("Score fetch task panicked: {}", e),
        }
    }
    
    if all_updates.is_empty() {
        return Ok(0);
    }
    
    // Chunked database operations with optimized retry
    let db_chunk_size = std::cmp::min(500, all_updates.len());
    let mut total_processed = 0;
    
    for chunk in all_updates.chunks(db_chunk_size) {
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 3;
        
        loop {
            match state.database_service.bulk_upsert_scores(chunk).await {
                Ok(_) => {
                    // Clear successfully processed scores
                    let user_ids: Vec<_> = chunk.iter().map(|u| u.user_id).collect();
                    if let Err(e) = state.redis_service.clear_scores_batch(&user_ids).await {
                        warn!("Failed to clear scores batch: {}", e);
                    }
                    total_processed += chunk.len();
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        error!("Database write failed after {} retries: {}", MAX_RETRIES, e);
                        
                        // Move failed batch to DLQ
                        if let Err(dlq_err) = state.redis_service.move_to_dlq(chunk).await {
                            error!("Failed to move batch to DLQ: {}", dlq_err);
                        } else {
                            warn!("Moved {} updates to DLQ", chunk.len());
                        }
                        break;
                    }
                    
                    // Exponential backoff with jitter
                    let base_delay = Duration::from_millis(100 * 2_u64.pow(retry_count));
                    let jitter = Duration::from_millis(fastrand::u64(0..=50));
                    tokio::time::sleep(base_delay + jitter).await;
                    
                    warn!("Database write retry {} for {} updates", retry_count, chunk.len());
                }
            }
        }
    }
    
    Ok(total_processed)
}