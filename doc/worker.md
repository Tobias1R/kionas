# Worker Binary Documentation

## Overview
The `worker` binary is responsible for executing tasks and communicating with the Kionas server via gRPC.

## Features
- Connects to the server using TLS
- Handles assigned tasks and reports results
- Uses JWT authentication for secure operations

## Key Files
- `worker/src/main.rs`: Entry point for the worker binary
- `configs/server.toml`: Contains worker and server settings

## Example Usage
To run the worker:
```sh
target/debug/worker
```

## Troubleshooting
- **TLS Certificate Errors:** Ensure certificates are valid and match expected DNS names
- **Connection Issues:** Verify server address and port

## Further Documentation
Expand as new worker features and task types are added.
