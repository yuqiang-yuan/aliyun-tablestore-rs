use reqwest::StatusCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OtsError {
    #[error("{0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("{0}")]
    ProtobufDecodeError(#[from] prost::DecodeError),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Aliyun ots api response with non-successful code: {0}. response message is: {1}")]
    StatusError(StatusCode, String),
}
