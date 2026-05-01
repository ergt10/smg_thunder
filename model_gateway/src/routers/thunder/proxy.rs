//! HTTP forwarding helpers for ThunderRouter.
//!
//! Phase 4: chat passthrough for both non-streaming JSON and streaming SSE.

use axum::body::Body;
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderValue, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use openai_protocol::chat::ChatCompletionRequest;
use serde_json::{to_value, Value};

use crate::routers::common::header_utils;
use crate::routers::error;

pub(super) type StreamingFinishCallback = Box<dyn FnOnce(Option<u64>) + Send + 'static>;

pub(super) struct ForwardedChatResponse {
    pub response: Response,
    pub total_tokens: Option<u64>,
}

impl ForwardedChatResponse {
    fn new(response: Response, total_tokens: Option<u64>) -> Self {
        Self {
            response,
            total_tokens,
        }
    }
}

fn chat_completions_url(worker_url: &str) -> String {
    format!("{}/v1/chat/completions", worker_url.trim_end_matches('/'))
}

fn upstream_payload(body: &ChatCompletionRequest) -> Result<Value, serde_json::Error> {
    let mut payload = to_value(body)?;
    remove_program_id(&mut payload);
    Ok(payload)
}

fn remove_program_id(payload: &mut Value) {
    let Some(obj) = payload.as_object_mut() else {
        return;
    };

    obj.remove("program_id");
    if let Some(extra_body) = obj.get_mut("extra_body").and_then(Value::as_object_mut) {
        extra_body.remove("program_id");
        if extra_body.is_empty() {
            obj.remove("extra_body");
        }
    }
}

fn extract_total_tokens(payload: &Value) -> Option<u64> {
    payload
        .get("usage")
        .and_then(|usage| usage.get("total_tokens"))
        .and_then(Value::as_u64)
}

fn extract_total_tokens_from_bytes(bytes: &[u8]) -> Option<u64> {
    let payload = serde_json::from_slice::<Value>(bytes).ok()?;
    extract_total_tokens(&payload)
}

type UpstreamByteStream = BoxStream<'static, Result<Bytes, reqwest::Error>>;

struct StreamingProgramFinisher {
    inner: UpstreamByteStream,
    sse_buffer: String,
    total_tokens: Option<u64>,
    on_finish: Option<StreamingFinishCallback>,
}

impl StreamingProgramFinisher {
    fn new(inner: UpstreamByteStream, on_finish: Option<StreamingFinishCallback>) -> Self {
        Self {
            inner,
            sse_buffer: String::new(),
            total_tokens: None,
            on_finish,
        }
    }

    fn observe_chunk(&mut self, chunk: &[u8]) {
        self.sse_buffer.push_str(&String::from_utf8_lossy(chunk));
        while let Some(newline) = self.sse_buffer.find('\n') {
            let line = self.sse_buffer[..newline].trim_end_matches('\r').to_owned();
            self.sse_buffer.drain(..=newline);

            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let data = data.trim();
            if data.is_empty() || data == "[DONE]" {
                continue;
            }
            if let Ok(payload) = serde_json::from_str::<Value>(data) {
                if let Some(total_tokens) = extract_total_tokens(&payload) {
                    self.total_tokens = Some(total_tokens);
                }
            }
        }
    }

    fn finish(&mut self) {
        if let Some(on_finish) = self.on_finish.take() {
            on_finish(self.total_tokens);
        }
    }
}

impl Drop for StreamingProgramFinisher {
    fn drop(&mut self) {
        self.finish();
    }
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
) -> ForwardedChatResponse {
    let url = chat_completions_url(worker_url);

    let payload = match upstream_payload(body) {
        Ok(v) => v,
        Err(e) => {
            return ForwardedChatResponse::new(
                error::bad_request(
                    "validation_error",
                    format!("Failed to serialize chat completion body: {e}"),
                ),
                None,
            );
        }
    };

    let resp = match client.post(&url).json(&payload).send().await {
        Ok(r) => r,
        Err(e) => {
            return ForwardedChatResponse::new(
                error::service_unavailable(
                    "upstream_error",
                    format!("Failed to contact upstream {url}: {e}"),
                ),
                None,
            );
        }
    };

    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let content_type = resp.headers().get(CONTENT_TYPE).cloned();

    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return ForwardedChatResponse::new(
                error::service_unavailable(
                    "upstream_error",
                    format!("Failed to read upstream body from {url}: {e}"),
                ),
                None,
            );
        }
    };
    let total_tokens = extract_total_tokens_from_bytes(&bytes);

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    if let Some(ct) = content_type {
        response.headers_mut().insert(CONTENT_TYPE, ct);
    } else {
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    ForwardedChatResponse::new(response, total_tokens)
}

/// Forward a streaming chat-completion request to `worker_url` and relay SSE bytes unchanged.
///
/// Phase 4 intentionally does not parse or transform the stream. Later phases add token progress
/// accounting by wrapping this byte stream while preserving the original SSE frames.
pub(super) async fn forward_streaming_chat(
    client: &reqwest::Client,
    worker_url: &str,
    body: &ChatCompletionRequest,
    mut on_finish: Option<StreamingFinishCallback>,
) -> Response {
    let url = chat_completions_url(worker_url);

    let payload = match upstream_payload(body) {
        Ok(v) => v,
        Err(e) => {
            if let Some(on_finish) = on_finish.take() {
                on_finish(None);
            }
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
            if let Some(on_finish) = on_finish.take() {
                on_finish(None);
            }
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

    let upstream_stream = resp.bytes_stream().boxed();
    let finisher = StreamingProgramFinisher::new(upstream_stream, on_finish);
    let stream = stream::unfold(finisher, |mut finisher| async move {
        match finisher.inner.next().await {
            Some(Ok(bytes)) => {
                finisher.observe_chunk(&bytes);
                Some((Ok(bytes), finisher))
            }
            Some(Err(err)) => {
                finisher.finish();
                Some((Err(err), finisher))
            }
            None => {
                finisher.finish();
                None
            }
        }
    });

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}
