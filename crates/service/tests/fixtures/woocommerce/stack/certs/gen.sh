#!/usr/bin/env bash
# Local CA and a server certificate for localhost. Output is gitignored except
# ca.pem, which the tests trust.
set -euo pipefail
cd "$(dirname "$0")"
openssl genrsa -out ca.key 2048 2>/dev/null
openssl req -x509 -new -key ca.key -sha256 -days 3650 -subj "/CN=Pubky Woo Test CA" -out ca.pem
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -subj "/CN=localhost" -out server.csr
printf "subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n" > ext.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem -days 825 -sha256 -extfile ext.cnf 2>/dev/null
rm -f server.csr ext.cnf ca.srl
