pub mod jwt;
pub mod persistence;

use serde::{Deserialize, Serialize};
use tonic::{Request, Status, service::Interceptor};

pub struct NoOpInterceptor;

impl Interceptor for NoOpInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        Ok(request)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub username: String,
    pub password: String,    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Token {
    pub token: String,
    pub username: String,
    pub exp: usize,
}