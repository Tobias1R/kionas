#!/bin/bash
# gen_root_ca.sh - Generate a Root CA certificate and key

CANAME=${1:-Kionas-RootCA}

mkdir -p $CANAME
cd $CANAME

# Generate AES-encrypted private key
openssl genrsa -aes256 -out $CANAME.key 4096

# Create self-signed Root CA certificate (5 years)
openssl req -x509 -new -nodes -key $CANAME.key -sha256 -days 1826 -out $CANAME.crt -subj "/CN=Kionas Root Authority CA/C=BR/ST=EspiritoSanto/L=Vila Velha/O=Kionas"

echo "Root CA generated: $CANAME/$CANAME.crt and $CANAME/$CANAME.key"
