

pub async fn handle_tap_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<TapBatchRequest>,
) -> Result<Json<TapBatchResponse>, AppError> {
    let user_id = extract_user_id_from_headers(&headers)?;

    if request.taps.is_empty() || request.taps.len() > 50 {
        return Err(AppError::Validation("Invalid tap batch size (1-50)".into()));
    }

    let total_taps = request.taps.iter().map(|t| t.count).sum::<i32>() as i64;
    let total_energy_cost = request.taps.iter().map(|t| t.energy_cost).sum::<i32>();

    if total_taps <= 0 {
        return Err(AppError::Validation("Tap count must be positive".into()));
    }

    let tap_result: TapResult = state
        .redis_service
        .process_tap_atomic(user_id, total_taps, total_energy_cost)
        .await?;

    log_tap_event_fire_and_forget(&state, user_id, total_taps as i32, request.timestamp);

    // {
    //     let mut metrics = state.metrics.write().await;
    //     metrics.tap_events_processed += total_taps as u64;
    //     // ✅ FIX: Hapus `redis_operations` karena sudah tidak ada di struct AppMetrics
    // }

    Ok(Json(TapBatchResponse {
        success: true,
        processed_taps: total_taps as u32,
        current_score: tap_result.new_score,
        current_energy: tap_result.new_energy,
        message: "Taps processed successfully".to_string(),
    }))
}