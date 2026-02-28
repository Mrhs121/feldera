use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Configuration for reading data from an S2 stream.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct S2InputConfig {
    /// Base URL of the S2 service, e.g., `http://localhost:7070`.
    pub endpoint_url: String,

    /// S2 basin name.
    pub basin: String,

    /// S2 stream name.
    pub stream: String,

    /// Authentication token sent as `Authorization: Bearer <token>`.
    ///
    /// Keep this value in a secret reference in production.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,

    /// Sequence number to start reading from when no checkpoint is available.
    #[serde(default = "default_start_seq_num")]
    pub start_seq_num: u64,

    /// Timeout for establishing a stream request and receiving the initial
    /// response headers.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Delay before reconnecting after the stream ends or fails.
    #[serde(default = "default_reconnect_timeout_secs")]
    pub reconnect_timeout_secs: u64,
}

const fn default_start_seq_num() -> u64 {
    0
}

const fn default_request_timeout_secs() -> u64 {
    30
}

const fn default_reconnect_timeout_secs() -> u64 {
    1
}
