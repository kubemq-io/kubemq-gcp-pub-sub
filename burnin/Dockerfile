# Multi-stage, non-root burn-in image for the KubeMQ Google Cloud Pub/Sub
# burn-in harness. Mirrors kubemq-aws/burnin/Dockerfile, repointed to port 8899.
# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /burnin-gcp-pub-sub .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
RUN adduser -D -u 1000 burnin
COPY --from=builder /burnin-gcp-pub-sub /usr/local/bin/burnin-gcp-pub-sub
COPY burnin-config.yaml /etc/burnin/config.yaml

USER burnin
EXPOSE 8899

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8899/health || exit 1

ENTRYPOINT ["burnin-gcp-pub-sub"]
CMD ["--config", "/etc/burnin/config.yaml"]
