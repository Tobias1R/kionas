use crate::auth::persistence::Persistence;
use crate::auth::{Token, User};
use crate::warehouse::state::SharedData;

use jsonwebtoken::{EncodingKey, Header, encode};
use log::debug;
use serde::{Deserialize, Serialize};

use crate::session::Session;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status};

#[allow(clippy::module_inception)]
pub mod warehouse_auth_service {
    tonic::include_proto!("warehouse_auth_service");
}
use warehouse_auth_service::warehouse_auth_service_server::WarehouseAuthService;
use warehouse_auth_service::{
    AuthResponse, JwtAuthRequest, LogoutRequest, LogoutResponse, RefreshTokenRequest,
    RefreshTokenResponse, UserPassAuthRequest,
};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    company: String,
    exp: usize,
}

#[allow(dead_code)]
pub struct WarehouseAuthServiceBackend {
    shared_data: SharedData,
    session_manager: Arc<crate::session::SessionManager>,
    persistence: Persistence,
    data_path: String,
    users: AsyncMutex<Vec<User>>,
    tokens: AsyncMutex<Vec<Token>>,
    service_secret: String,
}

impl WarehouseAuthServiceBackend {
    pub async fn initialize(
        shared_data: SharedData,
        data_path: String,
        service_secret: String,
    ) -> Self {
        let session_manager = Arc::clone(&shared_data.lock().await.session_manager);
        let file_path_tokens = format!("{}/{}", data_path.clone(), "tokens.parquet");
        let file_path_users = format!("{}/{}", data_path.clone(), "users.parquet");
        // if users file does not exist, create it and create the kionas user
        if !std::path::Path::new(file_path_users.as_str()).exists() {
            let users = vec![User {
                username: "kionas".to_string(),
                password: "kionas".to_string(),
            }];
            Persistence::save_users(users.clone(), file_path_users.as_str()).unwrap();
        }
        // if tokens file does not exist, create it
        if !std::path::Path::new(file_path_tokens.as_str()).exists() {
            let tokens = vec![];
            Persistence::save_tokens(tokens.clone(), file_path_tokens.as_str()).unwrap();
        }
        let tokens = Persistence::load_tokens(file_path_tokens.as_str()).unwrap_or_default();
        let users = Persistence::load_users(file_path_users.as_str()).unwrap_or_default();
        Self {
            shared_data: Arc::clone(&shared_data),
            session_manager,
            persistence: Persistence::new(data_path.clone()),
            data_path,
            users: AsyncMutex::new(users),
            tokens: AsyncMutex::new(tokens),
            service_secret,
        }
    }

    pub async fn new_session(
        &self,
        token: String,
        username: String,
        warehouse_name: String,
        remote_addr: String,
    ) -> Session {
        let hash_str = format!(
            "{}{}{}",
            token.clone(),
            username.clone(),
            warehouse_name.clone()
        );
        let session_id = kionas::gen_uuid_based_on_timestamp_and_string(hash_str.as_str());
        let pool_members = {
            let state = self.shared_data.lock().await;
            state
                .get_pool_members(&warehouse_name)
                .await
                .unwrap_or_default()
        };
        Session::new(
            session_id,
            token,
            username,
            warehouse_name,
            pool_members,
            remote_addr,
        )
    }
}

#[tonic::async_trait]
impl WarehouseAuthService for WarehouseAuthServiceBackend {
    async fn jwt_auth(
        &self,
        request: Request<JwtAuthRequest>,
    ) -> Result<Response<AuthResponse>, Status> {
        // Implement your JWT authentication logic here
        /*
        message AuthResponse {
            string message = 1; // The result of the authentication
            string status = 2; // The status of the authentication
            string error_code = 3; // The error code
            string execution_time = 4; // The execution time
            string token = 5; // The token
        }
         */

        let tokens = self.tokens.lock().await;
        let req = request.into_inner();
        let session = self
            .session_manager
            .get_token_session(req.token.clone())
            .await;
        if let Some(mut session) = session {
            if session.is_authenticated() {
                return Ok(Response::new(AuthResponse {
                    message: "Refreshed".to_string(),
                    status: "Success".to_string(),
                    error_code: "".to_string(),
                    execution_time: "0".to_string(),
                    token: req.token.clone(),
                    session_id: session.get_id(),
                }));
            }

            if let Some(token) = tokens.iter().find(|t| t.token == req.token) {
                // found on tokens, but not authenticated
                session.authenticate(req.token.clone());

                return Ok(Response::new(AuthResponse {
                    message: "Authenticated".to_string(),
                    status: "Success".to_string(),
                    error_code: "".to_string(),
                    execution_time: "0".to_string(),
                    token: token.token.clone(),
                    session_id: session.get_id(),
                }));
            }
        }

        Err(Status::unauthenticated("Invalid token ERR-1"))
    }

    async fn user_pass_auth(
        &self,
        request: Request<UserPassAuthRequest>,
    ) -> Result<Response<AuthResponse>, Status> {
        let users = self.users.lock().await;
        let remote_addr = request.remote_addr().unwrap().to_string();
        let req = request.into_inner();
        if let Some(user) = users
            .iter()
            .find(|u| u.username == req.username && u.password == req.password)
        {
            let claims = Claims {
                sub: user.username.clone(),
                company: "Kionas".to_string(),
                exp: 10000000000,
            };
            let token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(self.service_secret.as_ref()),
            )
            .map_err(|_| Status::internal("Failed to generate token"))?;
            let mut tokens = self.tokens.lock().await;
            tokens.push(Token {
                token: token.clone(),
                username: user.username.clone(),
                exp: claims.exp,
            });
            debug!("NEW Token: {:?}", token);
            let mut session = self
                .new_session(
                    token.clone(),
                    user.username.clone(),
                    "worker1".to_string(),
                    remote_addr.clone(),
                )
                .await;
            debug!("NEW Session: {:?}", session);
            session.authenticate(token.clone());
            let session_id = session.get_id();
            let _ = self.session_manager.add_session(session).await;

            let file_path = format!("{}/{}", self.data_path.clone(), "tokens.parquet");
            Persistence::save_tokens(tokens.clone(), file_path.as_str()).unwrap();
            let message = format!("Welcome, {}", user.username);
            let status = "Success".to_string();
            let error_code = "".to_string();
            let execution_time = "0".to_string();
            let token = token.clone();
            return Ok(Response::new(AuthResponse {
                message,
                status,
                error_code,
                execution_time,
                token,
                session_id,
            }));
        }
        Err(Status::unauthenticated("Invalid username or password"))
    }

    async fn refresh_token(
        &self,
        request: Request<RefreshTokenRequest>,
    ) -> Result<Response<RefreshTokenResponse>, Status> {
        let req = request.into_inner();
        let tokens = self.tokens.lock().await;
        if let Some(token) = tokens.iter().find(|t| t.token == req.token) {
            let claims = Claims {
                sub: token.username.clone(),
                company: "Kionas".to_string(),
                exp: 10000000000,
            };
            let new_token = encode(
                &Header::default(),
                &claims,
                &EncodingKey::from_secret(self.service_secret.as_ref()),
            )
            .map_err(|_| Status::internal("Failed to generate token"))?;
            let mut tokens = self.tokens.lock().await;
            tokens.push(Token {
                token: new_token.clone(),
                username: token.username.clone(),
                exp: claims.exp,
            });
            let file_path = format!("{}/{}", self.data_path.clone(), "tokens.parquet");
            Persistence::save_tokens(tokens.clone(), file_path.as_str()).unwrap();
            /*
            message RefreshTokenResponse {
                string message = 1; // The result of the refresh token
                string status = 2; // The status of the refresh token
                string error_code = 3; // The error code
                string execution_time = 4; // The execution time
                string token = 5; // The token
            } */
            let message = "Token refreshed".to_string();
            let status = "Success".to_string();
            let error_code = "".to_string();
            let execution_time = "0".to_string();

            return Ok(Response::new(RefreshTokenResponse {
                message,
                status,
                error_code,
                execution_time,
                token: new_token,
            }));
        }
        Err(Status::unauthenticated("Invalid token"))
    }

    async fn logout(
        &self,
        request: Request<LogoutRequest>,
    ) -> Result<Response<LogoutResponse>, Status> {
        let req = request.into_inner();
        let mut tokens = self.tokens.lock().await;
        if let Some(pos) = tokens.iter().position(|t| t.token == req.token) {
            tokens.remove(pos);
            let file_path = format!("{}/{}", self.data_path.clone(), "tokens.parquet");
            Persistence::save_tokens(tokens.clone(), file_path.as_str()).unwrap();
            let status = "Success".to_string();
            let error_code = "".to_string();
            let execution_time = "0".to_string();
            return Ok(Response::new(LogoutResponse {
                message: "Logged out successfully".to_string(),
                status,
                error_code,
                execution_time,
            }));
        }
        Err(Status::unauthenticated("Invalid token"))
    }
}
