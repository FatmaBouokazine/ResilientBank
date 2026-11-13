# mTLS certificate rotation

The payment service can run with transport TLS, and with mutual TLS when `REQUIRE_MTLS=true`.
The certificate, private key and client CA are served through a reloader, so rotating them
does not require a restart. That matters here more than usual: a restart is a gap in payment
ingestion, and the rotation exists to protect availability, not to trade it away.

## How reloading works

- At startup the configured files are loaded. If they are invalid the service refuses to
  start, because serving plaintext when TLS was requested is worse than not starting.
- A background checker polls the files every 5 seconds. When a file's size or modification
  time changes, the material is re-read and swapped under a lock.
- Each TLS handshake resolves the certificate and client CA for that connection, so the next
  connection after a reload uses the new material.
- Reload is **fail-safe**. If the replacement is invalid or half-written, the error is
  logged and the previous certificate keeps serving. A bad rotation cannot take the service
  down; it leaves it on the old, still-valid certificate until the operator fixes the files.

Relevant configuration:

| Variable | Meaning |
|---|---|
| `TLS_CERT` | server certificate chain (PEM) |
| `TLS_KEY` | server private key (PEM) |
| `TLS_CA` | CA used to verify client certificates (used when `REQUIRE_MTLS=true`) |
| `REQUIRE_MTLS` | require and verify a client certificate on every connection |

## Routine server certificate rotation

```bash
scripts/rotate-mtls.sh
```

This signs a new server certificate with the existing CA and atomically replaces
`server.crt` and `server.key`. The service picks it up within 5 seconds. Watch the logs for:

```json
{"msg":"tls material reloaded","cert":"/path/server.crt","require_mtls":true}
```

Verify what the server is actually presenting:

```bash
openssl s_client -connect localhost:50051 -servername payments-api </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -dates -serial
```

## Replacing the CA

Replacing the CA is a coordinated operation because the server uses it to verify client
certificates:

```bash
scripts/rotate-mtls.sh --new-ca
```

Then every client must present a certificate signed by the new CA, or its connection will be
rejected after the reload. Sequence it as:

1. Distribute the new CA to clients and have them obtain new client certificates.
2. Roll out the new server CA and certificate together.
3. Watch for failed handshakes during the overlap window; a client still on the old CA will
   be refused.

The server verifies clients against a single CA pool at a time, so there is no dual-CA
overlap period. If a zero-downtime CA change is required, the reloader needs to accept both
the old and new CA in the pool, which is a deliberate follow-up rather than something this
script pretends to do.

## Files and permissions

- The private key must be readable only by the service user (`0600`). The rotation script
  enforces this.
- `deploy/certs/` is the default location and is not committed. Do not put private keys in
  git.
- In Kubernetes the same files are normally mounted from a Secret. Replacing the Secret is
  enough; the kubelet updates the mounted files and the reloader observes the change.
