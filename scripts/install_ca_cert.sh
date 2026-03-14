#!/bin/bash

apt install -y ca-certificates
cp /workspace/certs/Kionas-RootCA.crt /usr/local/share/ca-certificates
update-ca-certificates