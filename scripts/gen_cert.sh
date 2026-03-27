#!/bin/bash
# gen_cert.sh - Generate a signed certificate for a service (server, worker, etc.)
# Usage: ./gen_cert.sh /workspace/certs/Kionas-RootCA/Kionas-RootCA.crt /workspace/certs/Kionas-RootCA/Kionas-RootCA.key kionas-worker3 certs/Kionas-RootCA

set -e

CA_CERT=${1:-certs/Kionas-RootCA.crt}
CA_KEY=${2:-certs/Kionas-RootCA.key}
CN=${3:-kionas-worker}
OUT_DIR=${4:-$CN}

mkdir -p $OUT_DIR

# Generate private key
openssl genrsa -out $OUT_DIR/$CN.key 4096

# Create CSR
openssl req -new -key $OUT_DIR/$CN.key -out $OUT_DIR/$CN.csr -subj "/CN=$CN/C=BR/ST=EspiritoSanto/L=Vila Velha/O=Kionas"

# Sign certificate with Root CA
openssl x509 -req -in $OUT_DIR/$CN.csr -CA $CA_CERT -CAkey $CA_KEY -CAcreateserial -out $OUT_DIR/$CN.crt -days 825 -sha256

echo "Certificate generated: $OUT_DIR/$CN.crt and $OUT_DIR/$CN.key (signed by $CA_CERT)"
