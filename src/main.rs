// src/main.rs

use axum::body::Body;
use axum::http::{HeaderValue, Method, Request};
use axum::routing::get;
use axum::Extension;
use axum::{routing::post, Router};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, Semaphore};
use tower::buffer::BufferLayer;
use tower::limit::RateLimitLayer;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

// Local modules
mod circuit_breaker;
mod config;
mod controllers;
mod database;
mod errors;
mod middleware;
mod models;
mod monitoring;
mod repositories;
mod routes;
mod services;
mod workers; // <-- TAMBAHKAN INI

use crate::controllers::auth_controller;
use crate::errors::{AppError, Result};
use crate::repositories::player_repository::PlayerRepository;
use crate::services::redis_service::RedisService;
// Gunakan fungsi dari modul background
use crate::workers::{adaptive_background_worker, dlq_retry_worker};

#[derive(Clone)]
pub struct AppState {
    pub redis_service: RedisService,
    pub player_repo: PlayerRepository,
    pub metrics: Arc<RwLock<AppMetrics>>,
    pub flush_semaphore: Arc<Semaphore>,
    pub circuit_breaker: Arc<RwLock<CircuitBreakerState>>,
}

#[derive(Debug, Default, Clone)]
pub struct AppMetrics {
    pub total_requests: u64,
    pub tap_events_processed: u64,
    pub database_writes: u64,
    pub failed_requests: u64,
    pub current_queue_size: u64,
    pub avg_response_time_ms: u64,
    pub worker_cycles_completed: u64,
    pub circuit_breaker_trips: u64,
}

#[derive(Debug, Default, Clone)]
pub struct CircuitBreakerState {
    pub redis_failures: u32,
    pub db_failures: u32,
    pub last_failure_time: Option<std::time::Instant>,
    pub is_redis_open: bool,
    pub is_db_open: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    if std::env::var("ENABLE_CONSOLE").is_ok() {
        console_subscriber::init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
            )
            .with_target(false)
            .json()
            .init();
    }

    dotenvy::dotenv().ok();

    // Initialize services with timeout
    let (redis_service, player_repo) = tokio::time::timeout(Duration::from_secs(30), async {
        let redis = RedisService::new().await?;
        let db = database::Database::new().await?;
        let player_repo = PlayerRepository::new(db).await?;
        Ok::<_, AppError>((redis, player_repo))
    })
    .await??;

    let metrics = Arc::new(RwLock::new(AppMetrics::default()));
    let circuit_breaker = Arc::new(RwLock::new(CircuitBreakerState::default()));

    let max_concurrent_flushes = std::env::var("MAX_CONCURRENT_FLUSHES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_cpus::get() * 2);

    let flush_semaphore = Arc::new(Semaphore::new(max_concurrent_flushes));
    info!(
        "🚦 Flush semaphore configured for {} concurrent operations",
        max_concurrent_flushes
    );

    let app_state = AppState {
        redis_service,
        player_repo,
        metrics,
        flush_semaphore,
        circuit_breaker,
    };

    // Start background workers
    let num_workers = std::env::var("BACKGROUND_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(num_cpus::get() * 2);

    for worker_id in 0..num_workers {
        tokio::spawn(adaptive_background_worker(app_state.clone(), worker_id));
    }
    tokio::spawn(dlq_retry_worker(app_state.clone()));
    // Start monitoring tasks
    tokio::spawn(monitoring::health_monitor(app_state.clone()));
    tokio::spawn(monitoring::metrics_collector(app_state.clone()));
    tokio::spawn(monitoring::circuit_breaker_monitor(app_state.clone()));

    // Configure middleware
    let rate_limit_per_minute = std::env::var("RATE_LIMIT_PER_MINUTE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000);

    let buffer_size = std::env::var("BUFFER_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
let cors_layer = CorsLayer::new()
    .allow_origin("https://sdsd-ashy.vercel.app".parse::<HeaderValue>().unwrap())
    .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
    .allow_headers(tower_http::cors::Any);

    let middleware_stack = ServiceBuilder::new()
        .layer(BufferLayer::<Request<Body>>::new(buffer_size))
        .layer(RateLimitLayer::new(rate_limit_per_minute, Duration::from_secs(60)))
        .layer(TraceLayer::new_for_http()
            .make_span_with(|request: &Request<Body>| {
                tracing::info_span!("request", method = %request.method(), uri = %request.uri())
            }))
        .layer(RequestBodyLimitLayer::new(8 * 1024))
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new().gzip(true).br(false));

    info!(
        "🛡️ Middleware configured: buffer={}, rate_limit={}/min",
        buffer_size, rate_limit_per_minute
    );

    // Configure routes
    let public_routes = Router::new()
        .route("/api/v1/health", axum::routing::get(routes::health_check))
        .route("/api/v1/auth", axum::routing::post(routes::auth_telegram));

    // Router private (pakai middleware dummy_user_id_auth)
    let private_routes = Router::new()
        .route("/api/v1/tap", post(routes::handle_tap_batch))
        .route("/api/v1/user", get(routes::get_user_by_id_handler))
        .route("/api/v1/metrics", get(routes::get_metrics))
        .layer(axum::middleware::from_fn(middleware::verify_auth))
        .with_state(app_state.clone());

    let app = public_routes
        .merge(private_routes)
        .layer(cors_layer)
        .with_state(app_state.clone());
    // Configure TCP listener
    let addr: SocketAddr = "0.0.0.0:3001".parse()?;
    let listener = {
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
        socket.set_reuse_address(true)?;

        #[cfg(target_os = "linux")]
        {
            socket.set_reuse_port(true)?;
            socket.set_recv_buffer_size(262144)?;
            socket.set_send_buffer_size(262144)?;
            socket.set_tcp_nodelay(true)?;
        }
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;

        let backlog = std::env::var("TCP_BACKLOG")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4096);
        socket.listen(backlog)?;

        TcpListener::from_std(socket.into())?
    };

    info!(
        "🚀 Server listening on {} with {} workers",
        addr, num_workers
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
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
    info!("🛑 Graceful shutdown initiated.");
}
