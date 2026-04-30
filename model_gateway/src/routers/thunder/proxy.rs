//! HTTP forwarding helpers for ThunderRouter.
//!
//! Phase 4: chat passthrough for both non-streaming JSON and streaming SSE.

use axum::body::Body;
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::to_value;

use crate::routers::common::header_utils;
use crate::routers::error;

fn chat_completions_url(worker_url: &str) -> String {
    format!("{}/v1/chat/completions", worker_url.trim_end_matches('/'))
}

/// Forward a non-streaming chat-completion request to `worker_url` and relay the response.
///
/// `worker_url` is the upstream's base (e.g. `http://localhost:8001`); we append
/// `/v1/chat/completions`. On any HTTP/IO failure returns a 503 axum response so the caller
/// (and ultimately the client) sees a clear failure mode rather than a hung connection.
pub(super) async fn forward_non_streaming_chat(
    client: &reqwest::Client,
    worker_url: &str,
    body: &ChatCompletionRequest,
) -> Response {
    let url = chat_completions_url(worker_url);

    let payload = match to_value(body) {
        Ok(v) => v,
        Err(e) => {
            return error::bad_request(
                "validation_error",
                format!("Failed to serialize chat completion body: {e}"),
            );
        }
    };

    let resp = match client.post(&url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            return error::service_unavailable(
                "upstream_error",
                format!("Failed to contact upstream {url}: {e}"),
            );
        }
    };

    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let content_type = resp.headers().get(CONTENT_TYPE).cloned();

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return error::service_unavailable(
                "upstream_error",
                format!("Failed to read upstream body from {url}: {e}"),
            );
        }
    };

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    if let Some(ct) = content_type {
        response.headers_mut().insert(CONTENT_TYPE, ct);
    } else {
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    response
}

/// Forward a streaming chat-completion request to `worker_url` and relay SSE bytes unchanged.
///
/// Phase 4 intentionally does not parse or transform the stream. Later phases add token progress
/// accounting by wrapping this byte stream while preserving the original SSE frames.
pub(super) async fn forward_streaming_chat(
    client: &reqwest::Client,
    worker_url: &str,
    body: &ChatCompletionRequest,
) -> Response {
    let url = chat_completions_url(worker_url);

    let payload = match to_value(body) {
        Ok(v) => v,
        Err(e) => {
            return error::bad_request(
                "validation_error",
                format!("Failed to serialize chat completion body: {e}"),
            );
        }
    };

    let resp = match client
        .post(&url)
        .header(ACCEPT, HeaderValue::from_static("text/event-stream"))
        .json(&payload)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return error::service_unavailable(
                "upstream_error",
                format!("Failed to contact upstream {url}: {e}"),
            );
        }
    };

    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response_headers = header_utils::preserve_response_headers(resp.headers());
    response_headers
        .entry(CONTENT_TYPE)
        .or_insert(HeaderValue::from_static("text/event-stream"));

    let mut response = Response::new(Body::from_stream(resp.bytes_stream()));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}
