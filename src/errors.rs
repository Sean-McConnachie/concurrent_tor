use thiserror::Error;

use crate::request;

pub type RequestResult = Result<request::RequestResponse, RequestError>;

#[derive(Error, Debug)]
pub enum RequestError {
    #[error("Hyper error")]
    HyperError(#[from] hyper::Error),
    #[error("Serde error")]
    SerdeError(#[from] serde_json::Error),
    #[error("Option is none error")]
    OptionNoneError,
    #[error("No redirect found")]
    NoRedirect,
    #[error("hyper::ToStrError")]
    ToStrError(#[from] hyper::header::ToStrError),
    #[error("Invalid uri")]
    InvalidURL(#[from] hyper::http::uri::InvalidUri),
}
