use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
    Extension,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::services::auth_service::AuthService;
use crate::{
    errors::AppError,
    models::{
        player::{CreatePlayerRequest, PlayerResponse, ValidatedUser},
        telegram::*,
    },
    services::{redis_service::TapResult, telegram::TelegramService},
    AppState,
};

// =================================================================================
// STRUCT UNTUK REQUEST & RESPONSE
// =================================================================================

#[derive(Debug, Deserialize)]
pub struct TapBatchRequest {
    pub taps: Vec<TapEvent>,
    pub timestamp: i64,
}

#[derive(Debug, Deserialize)]
pub struct TapEvent {
    pub count: i32,
    pub energy_cost: i32,
}

#[derive(Debug, Serialize)]
pub struct TapBatchResponse {
    pub success: bool,
    pub processed_taps: u32,
    pub current_score: i64,
    pub current_energy: i32,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: i64,
    pub services: ServiceHealth,
    // ✅ Menggunakan struct SystemMetrics yang sudah diperbarui
    pub metrics: SystemMetrics,
}

#[derive(Debug, Serialize)]
pub struct ServiceHealth {
    pub redis: bool,
    pub scylla: bool,
    pub redis_response_time_ms: u64,
    pub scylla_response_time_ms: u64,
}
#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub user_id: i64,
    pub score: i64,
    pub energy: i32,
    pub is_new_user: bool,
}

// ✅ UPDATED: Struct SystemMetrics disesuaikan agar cocok dengan AppMetrics
#[derive(Debug, Serialize, Clone)]
pub struct SystemMetrics {
    pub total_requests: u64,
    pub tap_events_processed: u64,
    pub database_writes: u64,
    pub failed_requests: u64,
    pub current_queue_size: u64,
    pub avg_response_time_ms: u64,
    pub worker_cycles_completed: u64,
    pub circuit_breaker_trips: u64,
}

// =================================================================================
// HANDLER UTAMA: TAP BATCH
// =================================================================================

/// Mengekstrak user ID dari header.

/// Handler berperforma tinggi untuk memproses batch tap dari pengguna.
pub async fn handle_tap_batch(
    State(state): State<AppState>,
    Extension(user): Extension<ValidatedUser>,
    Json(request): Json<TapBatchRequest>,
) -> Result<Json<TapBatchResponse>, AppError> {
    let user_id = user.id;

    // Validasi request
    if request.taps.is_empty() || request.taps.len() > 50 {
        return Err(AppError::Validation("Invalid tap batch size (1-50)".into()));
    }

    let total_taps = request.taps.iter().map(|t| t.count).sum::<i32>() as i64;
    let total_energy_cost = request.taps.iter().map(|t| t.energy_cost).sum::<i32>();

    if total_taps <= 0 {
        return Err(AppError::Validation("Tap count must be positive".into()));
    }

    // // ✅ NEW: Cek dan buat user jika belum ada

    // Proses tap di Redis
    let tap_result: TapResult = state
        .redis_service
        .process_tap_atomic(user_id, total_taps, total_energy_cost)
        .await?;

    // Update metrics
    {
        let mut metrics = state.metrics.write().await;
        metrics.tap_events_processed += total_taps as u64;
    }

    Ok(Json(TapBatchResponse {
        success: true,
        processed_taps: total_taps as u32,
        current_score: tap_result.new_score,
        current_energy: tap_result.new_energy,
        message: "Taps processed successfully".to_string(),
    }))
}

// src/routes.rs
pub async fn auth_telegram(
    State(state): State<AppState>,
    Json(payload): Json<AuthRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    debug!("Starting auth_telegram for user_id: {}", payload.user.id);

    // Ambil bot token
    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN").map_err(|_| {
        error!("Missing TELEGRAM_BOT_TOKEN in env");
        anyhow::anyhow!("Missing TELEGRAM_BOT_TOKEN in env")
    })?;

    debug!("Bot token loaded successfully");

    let telegram_service = TelegramService::new(bot_token);

    // Verifikasi init data
    debug!("Verifying init_data: {}", payload.init_data);
    let is_valid = telegram_service
        .verify_init_data(&payload.init_data)
        .map_err(|e| {
            error!("Error during verify_init_data: {:?}", e);
            anyhow::anyhow!("verify_init_data error: {}", e)
        })?;

    if !is_valid {
        warn!(
            "Init data verification failed for user_id: {}",
            payload.user.id
        );
        return Err(AppError::Authentication("Invalid Telegram data".into()));
    }
    debug!("Init data verification passed");
    let user_id = payload.user.id;
    // Cek user di DB
    debug!("Checking if user exists in DB: {}", payload.user.id);

    match state.player_repo.get_user_by_id(user_id).await? {
        Some(user) => {
            debug!("User found in DB: {:?}", user);
        }
        None => {
            debug!("User not found in DB, will create new player");
            state
                .player_repo
                .create(CreatePlayerRequest {
                    user_id,
                    username: payload.user.username.clone(),
                    first_name: Some(payload.user.first_name.clone()),
                    referral_code: None,
                    referred_by_id: None,
                })
                .await?;
            info!("Created new player with ID: {}", user_id);
        }
    }

    let jwt_secret =
        std::env::var("JWT_SECRET").map_err(|_| anyhow::anyhow!("Missing JWT_SECRET in env"))?;

    let auth_service = AuthService::new(&jwt_secret);
    let (token, exp) = auth_service.generate_jwt(payload.user.id)?;

    Ok(Json(AuthResponse {
        token,
        user: payload.user.clone(), // clone disini
        expires_at: exp,
        user_id: payload.user.id,
    }))
}
pub async fn get_user_by_id_handler(
    State(state): State<AppState>,
    Extension(user): Extension<ValidatedUser>,
) -> Result<Json<PlayerResponse>, AppError> {
    let user_id = user.id;

    // Decode JWT untuk dapat user_id

    // Cari user di DB pakai user_id dari JWT
    match state.player_repo.get_user_by_id(user_id).await? {
        Some(player) => {
            let response = PlayerResponse {
                user_id: player.user_id,
                username: player.username.clone(),
                first_name: player.first_name.clone(),
                score: player.score.unwrap_or(0),
                level: player.level.unwrap_or(1),
                energy: player.energy.unwrap_or(0),
                max_energy: player.max_energy.unwrap_or(0),
                tap_value: player.tap_value.unwrap_or(0),
                energy_recharge_rate: player.energy_recharge_rate.unwrap_or(0),
                referral_code: player.referral_code.clone(),
                last_seen: player
                    .last_seen
                    .and_then(|ts| DateTime::<Utc>::from_timestamp_millis(ts.0)),
            };
            Ok(Json(response))
        }
        None => Err(AppError::Auth(format!(
            "User with id {} not found",
            user_id
        ))),
    }
}

pub async fn handle_sync_session(
    State(state): State<AppState>,
    Extension(user): Extension<ValidatedUser>,
) -> Result<Json<SyncResponse>, AppError> {
    let user_id = user.id;

    // Selalu coba panggil fungsi regenerasi. Jika berhasil, berarti user ada di cache.
    match state.redis_service.get_user_stats_with_regen(user_id).await {
        Ok(stats) => {
            // CACHE HIT: User ditemukan di Redis, energi sudah di-update dengan benar.
            info!("Cache hit for user {} (with regen)", user_id);
            Ok(Json(SyncResponse {
                user_id,
                score: stats.score,
                energy: stats.energy,
                is_new_user: false,
            }))
        }
        Err(_) => {
            // CACHE MISS: User tidak ada di Redis. Kita harus periksa database.
            info!("Cache miss for user {}. Checking database...", user_id);

            match state.player_repo.get_user_by_id(user_id).await? {
                Some(player) => {
                    // KASUS 1: Pemain lama yang kembali.
                    info!("Hydrating cache for returning user {}", user_id);

                    // Ambil data dari DB, berikan nilai default jika ada yang null
                    let energy_from_db = player.energy.unwrap_or(1000);
                    let score_from_db = player.score.unwrap_or(0);

                    // "Panaskan" kembali cache Redis dengan data dari DB
                    state
                        .redis_service
                        .warm_up_cache(user_id, score_from_db, energy_from_db)
                        .await?;

                    // PENTING: Panggil lagi fungsi regen SEKARANG setelah cache diisi.
                    // Ini untuk menghitung energi yang terkumpul selama dia offline dan mendapatkan nilai final.
                    let final_stats = state
                        .redis_service
                        .get_user_stats_with_regen(user_id)
                        .await?;

                    Ok(Json(SyncResponse {
                        user_id,
                        score: final_stats.score,
                        energy: final_stats.energy,
                        is_new_user: false,
                    }))
                }
                None => {
                    // KASUS 2: Pemain yang benar-benar baru.
                    info!("Creating new player with ID: {}", user_id);
                    state
                        .player_repo
                        .create(CreatePlayerRequest {
                            user_id,
                            username: None,
                            first_name: None,
                            referral_code: None,
                            referred_by_id: None,
                        })
                        .await?;

                    // Ambil stats awal (default) dari Redis, yang akan dibuat otomatis
                    let initial_stats = state.redis_service.get_user_stats(user_id).await?;

                    Ok(Json(SyncResponse {
                        user_id,
                        score: initial_stats.score,
                        energy: initial_stats.energy,
                        is_new_user: true,
                    }))
                }
            }
        }
    }
}

// =================================================================================
// HANDLER LAINNYA: HEALTH & METRICS
// =================================================================================

/// Endpoint untuk pemeriksaan kesehatan sistem.
pub async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let (redis_health, scylla_health) = tokio::join!(
        state.redis_service.health_check(),
        state.player_repo.health_check()
    );

    let (redis_ok, redis_time) =
        redis_health.map_or((false, 0), |h| (h.is_healthy, h.response_time_ms));
    let (scylla_ok, scylla_time) =
        scylla_health.map_or((false, 0), |h| (h.is_healthy, h.response_time_ms));

    let overall_healthy = redis_ok && scylla_ok;
    let status_code = if overall_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    // ✅ UPDATED: Salin semua field dari AppMetrics ke SystemMetrics
    let metrics_guard = state.metrics.read().await;
    let metrics = SystemMetrics {
        total_requests: metrics_guard.total_requests,
        tap_events_processed: metrics_guard.tap_events_processed,
        database_writes: metrics_guard.database_writes,
        failed_requests: metrics_guard.failed_requests,
        current_queue_size: metrics_guard.current_queue_size,
        avg_response_time_ms: metrics_guard.avg_response_time_ms,
        worker_cycles_completed: metrics_guard.worker_cycles_completed,
        circuit_breaker_trips: metrics_guard.circuit_breaker_trips,
    };

    let response = HealthResponse {
        status: if overall_healthy {
            "healthy"
        } else {
            "unhealthy"
        }
        .to_string(),
        timestamp: chrono::Utc::now().timestamp(),
        services: ServiceHealth {
            redis: redis_ok,
            scylla: scylla_ok,
            redis_response_time_ms: redis_time,
            scylla_response_time_ms: scylla_time,
        },
        metrics,
    };

    (status_code, Json(response))
}

/// Endpoint untuk mengambil metrik sistem saat ini.
pub async fn get_metrics(State(state): State<AppState>) -> Json<SystemMetrics> {
    // ✅ UPDATED: Salin semua field dari AppMetrics ke SystemMetrics
    let metrics_guard = state.metrics.read().await;
    let metrics = SystemMetrics {
        total_requests: metrics_guard.total_requests,
        tap_events_processed: metrics_guard.tap_events_processed,
        database_writes: metrics_guard.database_writes,
        failed_requests: metrics_guard.failed_requests,
        current_queue_size: metrics_guard.current_queue_size,
        avg_response_time_ms: metrics_guard.avg_response_time_ms,
        worker_cycles_completed: metrics_guard.worker_cycles_completed,
        circuit_breaker_trips: metrics_guard.circuit_breaker_trips,
    };

    Json(metrics)
}
