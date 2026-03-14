# Server Binary Documentation

## Overview
The `server` binary is a core component of the Kionas project. It provides gRPC services for warehouse operations, authentication, and interops, secured with TLS and JWT authentication.

## Features
- gRPC server with TLS support
- JWT-based authentication
- Reflection service for gRPC
- Multiple service endpoints: Warehouse, Auth, Interops

## Main Components
- **TLS Configuration:** Uses certificates for secure communication. Ensure the certificate's SAN matches the expected DNS name (e.g., "warehouse").
- **JWT Authentication:** Issues JWT tokens upon successful login. Tokens are used for subsequent requests.
- **Reflection Service:** Allows clients to query available gRPC services and methods.

## Key Files
- `server/src/main.rs`: Entry point for the server binary.
- `server/src/handlers.rs`: Builds and launches the gRPC servers, configures TLS, and adds services.
- `server/src/auth/`: Handles authentication logic, including JWT issuance and validation.
- `server/src/services/`: Contains gRPC service implementations for warehouse, auth, and interops.
- `configs/server.toml`: Configuration file for server settings.

## Example Usage
To run the server:
```sh
cargo run --bin server -- --config ${KIONAS_HOME}/configs/server.toml
```

## Troubleshooting
- **TLS Certificate Errors:** Ensure the certificate SAN matches the expected DNS name.
- **prost Version Mismatch:** All crates must depend on the same prost version to avoid trait errors.

## Authentication Flow
1. Client sends login request with username and password.
2. Server responds with an AuthResponse containing a JWT token.
3. Client uses the JWT token for subsequent requests.

## Further Documentation
Expand this document as new features and endpoints are added.
