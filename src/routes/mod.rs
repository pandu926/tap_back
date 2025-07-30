use axum::{
    extract::{Extension, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use tracing::{info, warn};

use crate::{
    models::{BatchPayload, ValidatedUser, TapEventLog},
    errors::{AppError, Result},
    AppState,
};

pub async fn handle_tap_batch(
    State(state): State<AppState>,
    Extension(user): Extension<ValidatedUser>,
    Json(payload): Json<BatchPayload>,
) -> Result<impl IntoResponse> {
    let user_id = user.id;
    let tap_count = payload.taps.len() as i64;

    // Anti-cheat validation
    validate_tap_batch(&payload, user_id)?;

    // Log raw events (optional)
    if let Err(e) = log_tap_events(&state, user_id, &payload).await {
        warn!("Failed to log tap events for user {}: {}", user_id, e);
    }

    // Process batch in Redis
    state.redis_service.process_tap_batch(user_id, tap_count).await?;

    info!("Processed {} taps for user {}", tap_count, user_id);

    Ok((StatusCode::ACCEPTED, "Batch received for processing"))
}

fn validate_tap_batch(payload: &BatchPayload, user_id: i64) -> Result<()> {
    if payload.taps.is_empty() {
        return Err(AppError::Validation("Empty batch not allowed".to_string()));
    }

    if payload.taps.len() > 1000 {
        return Err(AppError::AntiCheat(
            "Batch size exceeds maximum limit".to_string()
        ));
    }

    // Timestamp analysis - check for impossibly fast tapping
    if payload.taps.len() > 1 {
        let mut prev_timestamp = payload.taps[0].t;
        let mut intervals = Vec::new();

        for tap in &payload.taps[1..] {
            let interval = tap.t - prev_timestamp;
            if interval < 10 { // Less than 10ms between taps is suspicious
                return Err(AppError::AntiCheat(
                    "Impossibly fast tapping detected".to_string()
                ));
            }
            intervals.push(interval);
            prev_timestamp = tap.t;
        }

        // Check for too consistent intervals (bot behavior)
        if intervals.len() > 10 {
            let avg_interval: f64 = intervals.iter().sum::<i64>() as f64 / intervals.len() as f64;
            let variance: f64 = intervals
                .iter()
                .map(|&x| {
                    let diff = x as f64 - avg_interval;
                    diff * diff
                })
                .sum::<f64>() / intervals.len() as f64;
            
            let std_dev = variance.sqrt();
            
            // If standard deviation is too low, it might be a bot
            if std_dev < 5.0 && intervals.len() > 20 {
                warn!("Suspicious consistent tapping pattern for user {}", user_id);
                return Err(AppError::AntiCheat(
                    "Suspicious tapping pattern detected".to_string()
                ));
            }
        }
    }

    // Check tap values
    for tap in &payload.taps {
        if tap.v <= 0 || tap.v > 10 {
            return Err(AppError::Validation(
                "Invalid tap value".to_string()
            ));
        }
    }

    Ok(())
}

async fn log_tap_events(
    state: &AppState,
    user_id: i64,
    payload: &BatchPayload,
) -> Result<()> {
    let now = Utc::now();
    let total_taps: i32 = payload.taps.iter().map(|t| t.v).sum();
    
    let log = TapEventLog {
        event_time: now,
        user_id,
        tap_count: total_taps,
    };

    state.database_service.log_tap_event(&log).await?;
    Ok(())
}