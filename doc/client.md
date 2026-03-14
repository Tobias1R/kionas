# Client Binary Documentation

## Overview
The `client` binary is used to interact with the Kionas server, performing authentication and warehouse operations via gRPC.

## Features
- Sends login requests and receives JWT tokens
- Performs authenticated operations using JWT
- Connects securely using TLS certificates

## Key Files
- `client/src/main.rs`: Entry point for the client binary
- `configs/server.toml`: Contains server connection settings

## Example Usage
To run the client:
```sh
target/debug/client --username <USERNAME> --password <PASSWORD>
```

## Authentication Flow
1. Client sends login request with credentials
2. Receives AuthResponse with JWT token
3. Uses token for subsequent requests

## Troubleshooting
- **TLS Certificate Errors:** Ensure the certificate matches the expected DNS name
- **Connection Errors:** Check server address and port in configs

## Further Documentation
Expand as new client features are added.
