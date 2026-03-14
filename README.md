# Kionas

Kionas is a cloud-oriented, distributed database system inspired by Snowflake. It is designed to run natively on Kubernetes or Docker, with a modular architecture and four main builds:

- **server**: The main database server
- **kionas**: Core library with shared logic
- **worker**: Distributed compute/worker node
- **client**: CLI or SDK for interacting with the system
- **metastore**: Metastore server for metadata and authentication

## Key Technologies
- **Rust** for all core components
- **Apache DataFusion** and **deltalake-rs** for delta table support
- **MinIO** for S3-compatible object storage
- **Kafka** for event streaming
- **Redis** for caching and coordination

## Infrastructure
Kionas is designed to be deployed in cloud-native environments, leveraging:
- **Kubernetes** or **Docker Compose** for orchestration
- **MinIO** for storage
- **Kafka** for messaging
- **Redis** for fast data access

## Authentication
- JWT-based authentication for secure, multi-tenant access

## Project Structure
```
kionas/
  server/      # Main server binary
  kionas/  # Shared Rust library
  worker/      # Worker node binary
  client/      # CLI/SDK
  metastore/  # Metastore binary
```

---
*Replace this README with detailed docs as the project evolves.*
