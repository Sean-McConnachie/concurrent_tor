use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ParseError(String),
    MurmurHashError(String),
    SqlxError(sqlx::Error),
    SerdeError(String),
    ChannelError(String),
    HyperError(hyper::Error),
    HyperHttpError(hyper::http::Error),
    ArtiClientError(arti_client::Error),
    TokioNativeTlsError(tokio_native_tls::native_tls::Error),
    AnyhowError(anyhow::Error),
    RequestError(reqwest::Error),
    AsyncChannelError(String),
    FantocciniCmdError(fantoccini::error::CmdError),
    FantocciniNewSessionError(fantoccini::error::NewSessionError),
    Other(Box<dyn std::error::Error>),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

unsafe impl Send for Error {}
unsafe impl Sync for Error {}

impl From<fantoccini::error::CmdError> for Error {
    fn from(e: fantoccini::error::CmdError) -> Self {
        Error::FantocciniCmdError(e)
    }
}

impl From<fantoccini::error::NewSessionError> for Error {
    fn from(e: fantoccini::error::NewSessionError) -> Self {
        Error::FantocciniNewSessionError(e)
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(e: async_channel::RecvError) -> Self {
        Error::AsyncChannelError(e.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(e: async_channel::SendError<T>) -> Self {
        Error::AsyncChannelError(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::RequestError(e)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::AnyhowError(e)
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Error::SqlxError(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::ParseError(e.to_string())
    }
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Self {
        Error::HyperError(e)
    }
}

impl From<hyper::http::Error> for Error {
    fn from(e: hyper::http::Error) -> Self {
        Error::HyperHttpError(e)
    }
}

impl From<arti_client::Error> for Error {
    fn from(e: arti_client::Error) -> Self {
        Error::ArtiClientError(e)
    }
}

impl From<tokio_native_tls::native_tls::Error> for Error {
    fn from(e: tokio_native_tls::native_tls::Error) -> Self {
        Error::TokioNativeTlsError(e)
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Error::Other(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)))
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Error::Other(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Other(Box::new(e))
    }
}
