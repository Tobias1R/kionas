# Metastore Binary Documentation

## Overview
The `metastore` binary manages metadata storage and retrieval for the Kionas platform.

## Features
- Provides metadata services via gRPC
- Uses TLS for secure communication
- Integrates with SQL scripts for database initialization

## Key Files
- `metastore/src/main.rs`: Entry point for the metastore binary
- `scripts/metastore/init.sql`: SQL script for database setup
- `scripts/metastore/auth.sql`: SQL script for authentication setup

## Example Usage
To run the metastore:
```sh
target/debug/metastore
```

## Troubleshooting
- **Database Errors:** Ensure SQL scripts are executed and database is accessible
- **TLS Certificate Issues:** Verify certificate configuration

## Further Documentation
Expand as new metadata features are added.
