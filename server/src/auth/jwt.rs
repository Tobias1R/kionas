use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status, service::Interceptor};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    company: String,
    exp: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JwtInterceptor {
    secret: String,
}

impl JwtInterceptor {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    #[allow(clippy::result_large_err)]
    fn validate_token(&self, token: &str) -> Result<Claims, Status> {
        let validation = Validation::new(Algorithm::HS256);
        //println!("Validate Token: {:?}", token);
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_ref()),
            &validation,
        )
        .map_err(|_| Status::unauthenticated("Invalid token ERR-200654"))?;
        Ok(token_data.claims)
    }
}

impl Interceptor for JwtInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|header| header.to_str().ok())
            .and_then(|header| header.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("Missing or malformed token"))?;

        let claims = self.validate_token(token)?;

        if let Ok(subject) = tonic::metadata::MetadataValue::try_from(claims.sub.as_str()) {
            request.metadata_mut().insert("rbac_user", subject);
        }

        if let Some(_session_id) = request
            .metadata()
            .get("session_id")
            .and_then(|header| header.to_str().ok())
        {
            Ok(request)
        } else {
            Err(Status::unauthenticated("Missing session_id"))
        }
    }
}
