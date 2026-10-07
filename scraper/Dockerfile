# syntax=docker/dockerfile:1
#
# Go crawler. One Dockerfile for the scraper service, one build target per binary:
#   worker  cmd/worker   Temporal worker: fetches/renders pages, writes them to S3
#                        (or NDJSON / Kafka), optionally fanning out to Kafka
#   api     cmd/api      read-only documents API + Swagger UI over the S3 bucket
#   cli     cmd/scraper  one-shot client that starts a crawl workflow in Temporal
# e.g. `docker build --target api scraper/`; docker-compose.yml picks the target per
# service. The default (last) target is the worker. Build context: scraper/
#
# Every flag can be set through the environment variable of the same name
# (--s3-bucket == S3_BUCKET), which is how docker-compose.yml configures them.
# None of the binaries touches the PostgreSQL schema: the Python pgs-db migrations own it.

# Keep in step with the `go` directive in go.mod.
ARG GO_VERSION=1.26.6

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
# Pure-Go dependencies, so static binaries (CGO_ENABLED=0) run on distroless/static.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for cmd in worker api scraper; do \
        CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "/out/${cmd}" "./cmd/${cmd}" || exit 1; \
    done \
    && mkdir -p /out/data/output

# distroless/static: no shell or package manager; ships CA certificates (the worker
# fetches arbitrary HTTPS sites) and tzdata, and runs as the nonroot user (65532).
FROM gcr.io/distroless/static-debian13:nonroot AS api
COPY --from=build /out/api /app/api
EXPOSE 8080
ENTRYPOINT ["/app/api"]

FROM gcr.io/distroless/static-debian13:nonroot AS cli
COPY --from=build /out/scraper /app/scraper
# Seed lists for --seeds-file=/configs/<file> (docker-compose.yml also bind-mounts them).
COPY configs /configs
ENTRYPOINT ["/app/scraper"]

FROM gcr.io/distroless/static-debian13:nonroot AS worker
WORKDIR /app
COPY --from=build /out/worker /app/worker
# The NDJSON default --output is data/output/documents.ndjson, relative to /app.
COPY --from=build --chown=65532:65532 /out/data /app/data
# Prometheus metrics (--metrics-address)
EXPOSE 9090
ENTRYPOINT ["/app/worker"]
