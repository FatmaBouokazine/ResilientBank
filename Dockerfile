# Multi-stage build for the ResilientBank services.
#
# WHY CGO_ENABLED=0 MATTERS HERE:
# The Kafka client is franz-go, a pure-Go implementation, so the binaries link statically
# with no C toolchain and no shared library dependencies. That lets the runtime stage be a
# distroless static image with no libc at all, which is both smaller and removes the
# class of "works on my machine, missing symbol on someone else's" failures that a cgo
# dependency would reintroduce.

# ---- build stage -------------------------------------------------------------
FROM golang:1.27 AS builder

WORKDIR /src

# Dependencies are copied first so the module download layer is reused whenever only source
# changes. Copying the whole tree first would re-download on every code edit.
COPY services/payments-api/go.mod services/payments-api/go.sum ./
RUN go mod download

COPY services/payments-api/ ./

ARG VERSION=dev
ARG COMMIT=unknown

# Trimming paths and stripping symbols keeps the image small and reproducible. -s -w also
# removes the symbol table that would otherwise leak build-host paths.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
        -o /out/payments-api  ./cmd/payments-api && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
        -o /out/outbox-relayer ./cmd/outbox-relayer && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
        -o /out/dashboard ./cmd/dashboard && \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath -ldflags="-s -w" \
        -o /out/migrate        ./cmd/migrate

# ---- test stage --------------------------------------------------------------
# Tests run during the image build so a broken unit test blocks the image, not just CI's
# later job. Integration tests are excluded: they need a live database.
FROM builder AS test
RUN go test ./...

# ---- runtime stage -----------------------------------------------------------
# Distroless static: no shell, no package manager, no libc. A compromised process has no
# interpreter to escalate with.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

# The migrator runs as a Kubernetes Job, so it ships in the same image rather than needing
# its own build and version to keep in step.
COPY --from=builder /out/payments-api   /usr/local/bin/payments-api
COPY --from=builder /out/outbox-relayer /usr/local/bin/outbox-relayer
COPY --from=builder /out/dashboard      /usr/local/bin/dashboard
COPY --from=builder /out/migrate        /usr/local/bin/migrate

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/payments-api"]

