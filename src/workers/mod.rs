// src/worker.rs

use std::time::Duration;
use tracing::{error, info, instrument, warn};

use crate::{
    circuit_breaker::{
        reset_circuit_breaker_success, update_circuit_breaker_on_db_error,
        update_circuit_breaker_on_redis_error,
    },
    errors::{AppError, Result},
    models::player::UserScoreUpdate,
    AppState,
};

#[instrument(skip(state), fields(worker_id))]
pub async fn adaptive_background_worker(state: AppState, worker_id: usize) {
    let min_interval = Duration::from_millis(100);
    let max_interval = Duration::from_secs(2);
    let mut current_interval = min_interval;
    let mut consecutive_errors = 0u32;
    const MIN_FLUSH_THRESHOLD: usize = 100;
    const MAX_FLUSH_WAIT_TIME: Duration = Duration::from_secs(30);
    info!("🔧 Worker {} started", worker_id);
    let mut last_flush_time = std::time::Instant::now();
    loop {
        tokio::time::sleep(current_interval).await;

        let circuit_state = state.circuit_breaker.read().await;
        if circuit_state.is_redis_open || circuit_state.is_db_open {
            warn!("Worker {}: Circuit breaker is open, backing off", worker_id);
            drop(circuit_state);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        drop(circuit_state);

        let permit = match state.flush_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Semaphore penuh, skip siklus ini agar tidak menumpuk.
                continue;
            }
        };

        let queue_size = state.redis_service.get_queue_size().await.unwrap_or(0);
        let should_flush = queue_size > 0 && // Hanya flush jika ada sesuatu
            (queue_size >= MIN_FLUSH_THRESHOLD || last_flush_time.elapsed() >= MAX_FLUSH_WAIT_TIME);

        if !should_flush {
            // Tidak ada kondisi untuk flush, perlakukan sebagai "tidak ada pekerjaan"
            current_interval = (current_interval * 2).min(max_interval);
            continue;
        }
        let batch_size = calculate_optimal_batch_size(queue_size);

        let cycle_start = std::time::Instant::now();
        match flush_and_persist(&state, batch_size, worker_id).await {
            Ok(count) => {
                consecutive_errors = 0;
                current_interval = min_interval;
                last_flush_time = std::time::Instant::now();
                let cycle_duration = cycle_start.elapsed();
                info!(
                    "Worker {}: Processed {} items in {}ms (queue: {})",
                    worker_id,
                    count,
                    cycle_duration.as_millis(),
                    queue_size
                );

                let mut metrics = state.metrics.write().await;
                metrics.database_writes += count as u64;
                metrics.current_queue_size = queue_size as u64;
                metrics.worker_cycles_completed += 1;
            }
            Err(e) => {
                consecutive_errors += 1;
                error!("Worker {}: Error #{}: {}", worker_id, consecutive_errors, e);

                let backoff_ms = 100 * 2_u64.pow(consecutive_errors.min(6)) + fastrand::u64(0..100);
                current_interval = Duration::from_millis(backoff_ms).min(max_interval);

                state.metrics.write().await.failed_requests += 1;
            }
        }
        drop(permit);
    }
}

fn calculate_optimal_batch_size(queue_size: usize) -> usize {
    match queue_size {
        0..=500 => 1000,
        501..=2000 => 3000,
        2001..=10000 => 5000,
        10001..=50000 => 10000,
        _ => 15000,
    }
}

#[instrument(skip_all)]
pub async fn dlq_retry_worker(state: AppState) {
    info!("🔧 DLQ Retry Worker started. Checking every 5 minutes.");

    // Worker ini berjalan lebih jarang karena bukan tugas kritis real-time
    let mut interval = tokio::time::interval(Duration::from_secs(300)); // Setiap 5 menit

    loop {
        interval.tick().await;

        let queue_size = match state.redis_service.get_queue_size_dlq().await {
            Ok(size) => size,
            Err(e) => {
                error!("DLQ Worker: Failed to get DLQ queue size: {}", e);
                continue;
            }
        };

        if queue_size == 0 {
            continue; // Tidak ada pekerjaan, kembali tidur
        }

        info!(
            "DLQ Worker: Found {} items in DLQ. Attempting to re-process a batch.",
            queue_size
        );

        // 1. Intip DLQ untuk mendapatkan kandidat
        let user_ids_to_retry = match state.redis_service.peek_dlq_users(50).await {
            Ok(ids) => ids,
            Err(e) => {
                error!("DLQ Worker: Failed to peek DLQ users: {}", e);
                continue;
            }
        };

        if user_ids_to_retry.is_empty() {
            continue;
        }

        // 2. Ambil data lengkapnya
        let updates_to_retry = match state
            .redis_service
            .get_dlq_user_scores(&user_ids_to_retry)
            .await
        {
            Ok(updates) => updates,
            Err(e) => {
                error!("DLQ Worker: Failed to get DLQ user scores: {}", e);
                continue;
            }
        };

        // 3. Coba simpan kembali ke database
        match state
            .player_repo
            .bulk_upsert_scores(&updates_to_retry)
            .await
        {
            Ok(_) => {
                // 4a. Jika SUKSES, hapus dari DLQ
                info!(
                    "DLQ Worker: Successfully re-processed {} items.",
                    updates_to_retry.len()
                );
                if let Err(e) = state.redis_service.remove_from_dlq(&updates_to_retry).await {
                    error!(
                        "DLQ Worker: CRITICAL! Failed to remove processed items from DLQ: {}",
                        e
                    );
                }
            }
            Err(e) => {
                // 4b. Jika GAGAL lagi, biarkan saja di DLQ untuk percobaan berikutnya.
                warn!(
                    "DLQ Worker: Re-processing failed, will retry on next cycle. Error: {}",
                    e
                );
            }
        }
    }
}

async fn flush_and_persist(state: &AppState, batch_size: usize, worker_id: usize) -> Result<usize> {
    // 1. Ambil data dari Redis
    let updates = match tokio::time::timeout(
        Duration::from_secs(5),
        state
            .redis_service
            .get_and_clear_dirty_scores_batch_resilient(batch_size),
    )
    .await
    {
        Ok(Ok(updates)) => updates,
        Ok(Err(e)) => {
            error!("Worker {}: Redis error: {}", worker_id, e);
            update_circuit_breaker_on_redis_error(state).await;
            return Err(e);
        }
        Err(_) => {
            error!("Worker {}: Redis timeout", worker_id);
            update_circuit_breaker_on_redis_error(state).await;
            return Err(AppError::Internal(anyhow::anyhow!(
                "Redis operation timeout"
            )));
        }
    };

    let processed_count = updates.len();
    if processed_count == 0 {
        return Ok(0);
    }

    // 2. Simpan data ke Database
    match tokio::time::timeout(
        Duration::from_secs(15),
        state.player_repo.bulk_upsert_scores(&updates),
    )
    .await
    {
        Ok(Ok(_)) => {
            reset_circuit_breaker_success(state).await;
            Ok(processed_count)
        }
        Ok(Err(e)) => {
            error!("Worker {}: Database error: {}. Moving to DLQ", worker_id, e);
            update_circuit_breaker_on_db_error(state).await;
            move_to_dlq(state, &updates, worker_id).await;
            Err(e.into())
        }
        Err(_) => {
            error!("Worker {}: Database timeout. Moving to DLQ", worker_id);
            update_circuit_breaker_on_db_error(state).await;
            move_to_dlq(state, &updates, worker_id).await;
            Err(AppError::Internal(anyhow::anyhow!(
                "Database operation timeout"
            )))
        }
    }
}

// Fungsi helper untuk memindahkan ke DLQ
async fn move_to_dlq(state: &AppState, updates: &[UserScoreUpdate], worker_id: usize) {
    if let Err(dlq_err) = tokio::time::timeout(
        Duration::from_secs(5),
        state.redis_service.move_to_dlq(updates),
    )
    .await
    {
        error!(
            "Worker {}: CRITICAL! DLQ operation failed: {:?}",
            worker_id, dlq_err
        );
    }
}
