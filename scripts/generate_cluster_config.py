import json
import os

cluster_config = {
    "nodes": [os.getenv("WORKER_HOST", "kionas-worker1")],
    "master": "https://kionas-warehouse:50051",
    "server": {
        "listen_address": os.getenv("SERVER_ADDRESS", "172.18.0.7"),
        "listen_port": int(os.getenv("SERVER_PORT", "443")),
        "consul_host": os.getenv("CONSUL_HOST", "kionas-consul")
    },
    "logging": {
        "level": os.getenv("LOG_LEVEL", "info"),
        "format": os.getenv("LOG_FORMAT", "text"),
        "output": os.getenv("LOG_OUTPUT", "stdout")
    },
    "security": {
        "token": os.getenv("JWT_TOKEN", "jwt"),
        "secret": os.getenv("JWT_SECRET", "That70sShow"),
        "data_path": os.getenv("DATA_PATH", "/workspace/data")
    },
    "storage": {
        "storage_type": os.getenv("STORAGE_TYPE", "minio"),
        "bucket": os.getenv("STORAGE_BUCKET", "warehouse"),
        "region": os.getenv("STORAGE_REGION", "us-east-1"),
        "endpoint": os.getenv("STORAGE_ENDPOINT", "http://kionas-minio:9000"),
        "access_key": os.getenv("STORAGE_ACCESS_KEY", "root"),
        "secret_key": os.getenv("STORAGE_SECRET_KEY", "rootpassword")
    },
    "warehouse": {
        "host": os.getenv("WAREHOUSE_HOST", "kionas-warehouse.warehouse_network"),
        "port": int(os.getenv("WAREHOUSE_PORT", "443")),
        "tls_cert": os.getenv("WAREHOUSE_TLS_CERT", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/warehouse.local.crt"),
        "tls_key": os.getenv("WAREHOUSE_TLS_KEY", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/warehouse.local.key")
    },
    "interops": {
        "host": os.getenv("INTEROPS_HOST", "kionas-warehouse.warehouse_network"),
        "port": int(os.getenv("INTEROPS_PORT", "50051")),
        "tls_cert": os.getenv("INTEROPS_TLS_CERT", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/warehouse.local.crt"),
        "tls_key": os.getenv("INTEROPS_TLS_KEY", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/warehouse.local.key"),
        "ca_cert": os.getenv("INTEROPS_CA_CERT", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/Kionas-RootCA.crt")
    },
    "metastore": {
        "host": os.getenv("METASTORE_HOST", "kionas-metastore"),
        "port": int(os.getenv("METASTORE_PORT", "443")),
        "tls_cert": os.getenv("METASTORE_TLS_CERT", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/metastore.local.crt"),
        "tls_key": os.getenv("METASTORE_TLS_KEY", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/metastore.local.key"),
        "ca_cert": os.getenv("METASTORE_CA_CERT", f"{os.getenv('KIONAS_HOME')}/certs/Kionas-RootCA/Kionas-RootCA.crt")
    }
}

with open("/workspace/configs/cluster.json", "w") as f:
    json.dump(cluster_config, f, indent=2)
    print("Generated /workspace/configs/cluster.json")
