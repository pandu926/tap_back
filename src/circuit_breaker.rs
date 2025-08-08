// src/circuit_breaker.rs

use crate::AppState;
use tracing::{info, warn};

pub async fn update_circuit_breaker_on_error(state: &AppState, error: &anyhow::Error) {
    let error_msg = error.to_string().to_lowercase();
    if error_msg.contains("redis") {
        update_circuit_breaker_on_redis_error(state).await;
    } else if error_msg.contains("database") || error_msg.contains("db") {
        update_circuit_breaker_on_db_error(state).await;
    }
}

pub async fn update_circuit_breaker_on_redis_error(state: &AppState) {
    let mut circuit = state.circuit_breaker.write().await;
    circuit.redis_failures += 1;
    circuit.last_failure_time = Some(std::time::Instant::now());

    if circuit.redis_failures >= 5 && !circuit.is_redis_open {
        circuit.is_redis_open = true;
        warn!(
            "🚨 Redis circuit breaker opened after {} failures",
            circuit.redis_failures
        );

        let mut metrics = state.metrics.write().await;
        metrics.circuit_breaker_trips += 1;
    }
}

pub async fn update_circuit_breaker_on_db_error(state: &AppState) {
    let mut circuit = state.circuit_breaker.write().await;
    circuit.db_failures += 1;
    circuit.last_failure_time = Some(std::time::Instant::now());

    if circuit.db_failures >= 5 && !circuit.is_db_open {
        circuit.is_db_open = true;
        warn!(
            "🚨 Database circuit breaker opened after {} failures",
            circuit.db_failures
        );

        let mut metrics = state.metrics.write().await;
        metrics.circuit_breaker_trips += 1;
    }
}

pub async fn reset_circuit_breaker_success(state: &AppState) {
    let mut circuit = state.circuit_breaker.write().await;
    if circuit.redis_failures > 0 || circuit.db_failures > 0 {
        circuit.redis_failures = 0;
        circuit.db_failures = 0;
        // Jangan tutup circuit breaker di sini, biarkan monitor yang melakukannya
        info!("✅ Successful operation, resetting failure counts.");
    }
}
