// src/monitoring.rs

use std::time::Duration;
use tracing::{error, info, instrument, warn};

use crate::{
    circuit_breaker::{update_circuit_breaker_on_db_error, update_circuit_breaker_on_redis_error},
    AppState,
};

#[instrument(skip(state))]
pub async fn health_monitor(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(15));

    loop {
        interval.tick().await;

        let health_timeout = Duration::from_secs(5);
        let (redis_result, db_result) = tokio::join!(
            tokio::time::timeout(health_timeout, state.redis_service.health_check()),
            tokio::time::timeout(health_timeout, state.player_repo.health_check())
        );

        match redis_result {
            Ok(Ok(_)) => { /* Healthy */ }
            Ok(Err(e)) => {
                error!("❌ Redis health check failed: {}", e);
                update_circuit_breaker_on_redis_error(&state).await;
            }
            Err(_) => {
                error!("❌ Redis health check timeout");
                update_circuit_breaker_on_redis_error(&state).await;
            }
        }

        match db_result {
            Ok(Ok(_)) => { /* Healthy */ }
            Ok(Err(e)) => {
                error!("❌ Database health check failed: {}", e);
                update_circuit_breaker_on_db_error(&state).await;
            }
            Err(_) => {
                error!("❌ Database health check timeout");
                update_circuit_breaker_on_db_error(&state).await;
            }
        }
    }
}

#[instrument(skip(state))]
pub async fn metrics_collector(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if let Ok(queue_size) = state.redis_service.get_queue_size().await {
            let mut metrics = state.metrics.write().await;
            metrics.current_queue_size = queue_size as u64;

            if queue_size > 10000 {
                warn!("⚠️  High queue backlog: {} items", queue_size);
            }
        }

        let metrics = state.metrics.read().await;
        info!(
            "📊 Metrics: requests={}, processed={}, queue={}, errors={}",
            metrics.total_requests,
            metrics.tap_events_processed,
            metrics.current_queue_size,
            metrics.failed_requests
        );
    }
}

#[instrument(skip(state))]
pub async fn circuit_breaker_monitor(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        let mut circuit = state.circuit_breaker.write().await;
        let now = std::time::Instant::now();

        let was_open = circuit.is_redis_open || circuit.is_db_open;

        // Coba tutup circuit breaker jika sudah pulih
        if let Some(last_failure) = circuit.last_failure_time {
            if now.duration_since(last_failure) > Duration::from_secs(30) {
                if circuit.is_redis_open {
                    circuit.is_redis_open = false;
                    circuit.redis_failures = 0;
                    info!("🔄 Trying to close Redis circuit breaker (half-open state).");
                }
                if circuit.is_db_open {
                    circuit.is_db_open = false;
                    circuit.db_failures = 0;
                    info!("🔄 Trying to close Database circuit breaker (half-open state).");
                }
            }
        }

        if was_open && !circuit.is_redis_open && !circuit.is_db_open {
            info!("✅ Circuit breakers have been closed after recovery period.");
        }

        if circuit.is_redis_open || circuit.is_db_open {
            warn!(
                "🚨 Circuit breakers status: Redis={}, Database={}",
                circuit.is_redis_open, circuit.is_db_open
            );
        }
    }
}
