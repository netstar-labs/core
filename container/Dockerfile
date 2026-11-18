# syntax=docker/dockerfile:1

# ---- build stage: compile a static binary ----
FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Build only the server (CGO off ⇒ a fully static binary that runs on bare Alpine).
COPY . .
ARG TARGETOS=linux TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/server ./server

# ---- final stage: Alpine with only the binary ----
FROM alpine:3.20
# Non-root user and a writable data dir for the store files.
RUN adduser -D -u 10001 app && mkdir -p /data && chown app:app /data
COPY --from=build /out/server /usr/local/bin/server
# BSD-3-Clause clause 2: the internal/mmap notice must travel with the binary.
COPY --from=build /src/THIRD-PARTY-NOTICES /THIRD-PARTY-NOTICES

USER app
VOLUME ["/data"]
EXPOSE 8080

# Liveness via the server's /status (busybox wget ships with Alpine). Assumes
# the in-container port stays 8080; adjust if you change -addr.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/status || exit 1

# Defaults; override via `docker run ... <flags>` (they replace CMD).
ENTRYPOINT ["/usr/local/bin/server"]
CMD ["-addr", ":8080", "-store", "/data/kv.core", "-shards", "8"]
