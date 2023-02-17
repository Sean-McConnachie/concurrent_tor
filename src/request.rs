use hyper;
use hyper::body::Bytes;
use hyper::http::uri::InvalidUri;

use crate::errors;

/// Used to determine what a `Circuit` should do with a `Task` after completing a request.
#[derive(Debug, PartialEq)]
pub enum RequestType {
    /// Discards the request.
    Ignore,
    /// Retry if the maximum number of tries on the request has not been exceeded.
    Standard,
    /// Try find the same page on the wayback machine.
    WebArchive,
}

/// Stores data about requests.
#[derive(Debug)]
pub struct Request {
    uri: hyper::Uri,
    headers: hyper::HeaderMap,
    method: hyper::Method,
    /// Should redirect if there is a `LOCATION` header present in the response.
    allow_redirect: bool,
    try_count: usize,
    /// How many times to retry. This includes `WebArchive` requests.
    max_tries: usize,
    next_attempt: RequestType,
}

// TODO: Find a better way to share the commonalities between hyper requests.
// TODO: Request builder.
impl Request {
    pub fn new(
        uri: hyper::Uri,
        headers: hyper::HeaderMap,
        method: hyper::Method,
        allow_redirect: bool,
        try_count: usize,
        max_tries: usize,
        next_attempt: RequestType,
    ) -> Request {
        Request {
            uri,
            headers,
            method,
            allow_redirect,
            try_count,
            max_tries,
            next_attempt,
        }
    }

    pub fn uri(mut self, uri: hyper::Uri) -> Self {
        self.uri = uri;
        self
    }

    pub fn headers(mut self, headers: hyper::HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub fn allow_redirect(mut self, allow: bool) -> Self {
        self.allow_redirect = allow;
        self
    }

    pub fn max_tries(mut self, tries: usize) -> Self {
        if tries <= 0 {
            panic!("Request.tries must be >= 1")
        }
        self.max_tries = tries;
        self
    }

    pub fn next_attempt(&mut self, next_attempt: RequestType) {
        self.next_attempt = next_attempt;
    }

    pub fn can_try(&self) -> bool {
        self.try_count < self.max_tries
    }

    pub fn inc_try(&mut self) {
        self.try_count += 1
    }

    pub fn get_next_attempt(&self) -> &RequestType {
        if self.can_try() == false {
            return &RequestType::Ignore;
        }
        &self.next_attempt
    }

    pub fn get_allow_redirect(&self) -> bool {
        self.allow_redirect
    }

    pub fn build_request(&self, uri: hyper::Uri) -> hyper::Request<hyper::Body> {
        let mut r = hyper::Request::builder()
            .uri(uri)
            .method(self.method.clone())
            .body(hyper::Body::empty()) // TODO: Add support for custom body
            .unwrap();
        for h in &self.headers {
            r.headers_mut().insert(h.0, h.1.into());
        }
        r
    }

    pub fn build_standard_request(&self) -> hyper::Request<hyper::Body> {
        self.build_request(self.uri.clone())
    }

    pub fn build_archive_request(&self) -> Result<hyper::Request<hyper::Body>, InvalidUri> {
        let wayback_uri = format!("http://archive.org/wayback/available?url={}", self.uri)
            .to_string()
            .parse::<hyper::Uri>()?;
        Ok(self.build_request(wayback_uri))
    }
}

impl Default for Request {
    fn default() -> Self {
        Request {
            uri: hyper::Uri::default(),
            headers: hyper::HeaderMap::with_capacity(0),
            method: hyper::Method::GET,
            allow_redirect: false,
            try_count: 0,
            max_tries: 1,
            next_attempt: RequestType::Standard,
        }
    }
}

/// Allows for a clean way to access the whole hyper response (i.e. status code, headers, body).
#[derive(Debug)]
pub struct RequestResponse {
    response: hyper::Response<hyper::Body>,
}

impl RequestResponse {
    pub fn new(response: hyper::Response<hyper::Body>) -> RequestResponse {
        RequestResponse { response }
    }

    pub async fn body_bytes(&mut self) -> Result<Bytes, errors::RequestError> {
        let bytes = hyper::body::to_bytes(self.response.body_mut()).await?;
        Ok(bytes)
    }

    pub fn response(&self) -> &hyper::Response<hyper::Body> {
        &self.response
    }
}
