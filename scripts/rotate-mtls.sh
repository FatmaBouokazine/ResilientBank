#!/usr/bin/env bash
set -euo pipefail

# scripts/rotate-mtls.sh
# Issues a fresh server certificate signed by the existing CA and writes it over the live
# files. The service reloads the certificate within its poll interval (5s by default) and
# keeps serving the old certificate if the new files are invalid.
#
# This rotates the SERVER certificate only. Rotating the CA additionally requires the
# clients to trust the new CA, so use --new-ca deliberately and coordinate it.
#
# Usage:
#   scripts/rotate-mtls.sh                 # new server cert, same CA
#   scripts/rotate-mtls.sh --new-ca        # new CA + new server cert
#   CERT_DIR=/etc/resilientbank/tls scripts/rotate-mtls.sh
#
# Environment:
#   CERT_DIR      directory holding ca.crt/ca.key/server.crt/server.key (default deploy/certs)
#   DAYS          validity in days (default 825)
#   CN            server certificate common name (default payments-api)

CERT_DIR="${CERT_DIR:-deploy/certs}"
CA_CERT="$CERT_DIR/ca.crt"
CA_KEY="$CERT_DIR/ca.key"
SERVER_CERT="$CERT_DIR/server.crt"
SERVER_KEY="$CERT_DIR/server.key"
DAYS="${DAYS:-825}"
CN="${CN:-payments-api}"

NEW_CA=0
[ "${1:-}" = "--new-ca" ] && NEW_CA=1

command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }
mkdir -p "$CERT_DIR"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [ "$NEW_CA" -eq 1 ] || [ ! -f "$CA_CERT" ] || [ ! -f "$CA_KEY" ]; then
    echo "Generating a new CA in $CERT_DIR"
    openssl genrsa -out "$work/ca.key" 4096 >/dev/null 2>&1
    openssl req -x509 -new -nodes -key "$work/ca.key" -sha256 -days "$DAYS" \
        -subj "/CN=ResilientBank CA" -out "$work/ca.crt"
    mv -f "$work/ca.key" "$CA_KEY"
    mv -f "$work/ca.crt" "$CA_CERT"
    chmod 600 "$CA_KEY"
    echo "CA written. Clients must trust $CA_CERT before they can connect again."
else
    echo "Signing with the existing CA at $CA_CERT"
fi

cat > "$work/server.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$CN,DNS:payments-api,DNS:localhost,IP:127.0.0.1
EOF

openssl genrsa -out "$work/server.key" 2048 >/dev/null 2>&1
openssl req -new -key "$work/server.key" -subj "/CN=$CN" -out "$work/server.csr"
openssl x509 -req -in "$work/server.csr" -CA "$CA_CERT" -CAkey "$CA_KEY" \
    -CAcreateserial -out "$work/server.crt" -days "$DAYS" -sha256 \
    -extfile "$work/server.ext" >/dev/null 2>&1

# Atomic replace: write a sibling file, then rename over the live path. The reloader never
# observes a half-written certificate.
mv -f "$work/server.key" "$SERVER_KEY.new"
mv -f "$work/server.crt" "$SERVER_CERT.new"
chmod 600 "$SERVER_KEY.new"
mv -f "$SERVER_KEY.new" "$SERVER_KEY"
mv -f "$SERVER_CERT.new" "$SERVER_CERT"

echo
echo "Rotated:"
echo "  certificate: $SERVER_CERT"
echo "  private key: $SERVER_KEY"
echo "  ca:          $CA_CERT"
echo
echo "The service reloads within its poll interval (5s). Watch for 'tls material reloaded'."
echo "Verify the served certificate with:"
echo "  openssl s_client -connect localhost:50051 -servername $CN </dev/null 2>/dev/null | openssl x509 -noout -subject -dates"
