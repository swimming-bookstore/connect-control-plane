#!/bin/sh
# Local CA + server cert for demo. Not for production.
set -eu
out=${1:-.}
mkdir -p "$out"
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$out/ca.key" -out "$out/ca.pem" -days 365 \
  -subj /CN=connect-control-plane-ca \
  -addext "basicConstraints=critical,CA:TRUE"
openssl req -newkey rsa:2048 -nodes \
  -keyout "$out/key.pem" -out "$out/server.csr" \
  -subj /CN=localhost
ext=$(mktemp)
printf 'basicConstraints=CA:FALSE\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n' >"$ext"
openssl x509 -req -in "$out/server.csr" -CA "$out/ca.pem" -CAkey "$out/ca.key" \
  -CAcreateserial -out "$out/cert.pem" -days 365 -extfile "$ext"
rm -f "$ext" "$out/server.csr" "$out/ca.srl"
echo "wrote $out/ca.pem $out/cert.pem $out/key.pem"
