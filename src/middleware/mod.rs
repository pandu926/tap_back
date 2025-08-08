use crate::{models::player::ValidatedUser, services::auth_service::AuthService};
use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::Response,
};

pub async fn verify_auth(mut request: Request, next: Next) -> Result<Response, StatusCode> {
    // Ambil token dari header
    let token = match request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(token) => token,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    // Ambil secret dari env
    let secret = std::env::var("JWT_SECRET").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let auth_service = AuthService::new(&secret);

    // Verifikasi JWT
    let user_id = match auth_service.decode_jwt(token) {
        Ok(user_id) => user_id,
        Err(_) => return Err(StatusCode::UNAUTHORIZED),
    };

    // Insert ValidatedUser
    let validated_user = ValidatedUser {
        id: user_id,
        username: None,   // bisa diisi nanti kalau perlu
        first_name: None, // bisa diisi nanti kalau perlu
        last_name: None,  // bisa diisi nanti kalau perlu
    };

    request.extensions_mut().insert(validated_user);

    Ok(next.run(request).await)
}
