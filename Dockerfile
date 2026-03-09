# Multi-stage Dockerfile — builds producer, consumer, and controlplane
# Usage:
#   docker build --target producer -t nats-poc-producer .
#   docker build --target consumer -t nats-poc-consumer .
#   docker build --target controlplane -t nats-poc-controlplane .        (Docker backend)
#   docker build --target controlplane-k8s -t nats-poc-controlplane .   (K8s backend)

# ---- Build stage ----
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM builder AS build-producer
RUN CGO_ENABLED=0 go build -o /bin/producer ./cmd/producer/

FROM builder AS build-consumer
RUN CGO_ENABLED=0 go build -o /bin/consumer ./cmd/consumer/

FROM builder AS build-controlplane
RUN CGO_ENABLED=0 go build -o /bin/controlplane ./cmd/controlplane/

# ---- Runtime images ----
FROM alpine:3.21 AS producer
RUN apk add --no-cache ca-certificates
COPY --from=build-producer /bin/producer /usr/local/bin/producer
ENTRYPOINT ["producer"]

FROM alpine:3.21 AS consumer
RUN apk add --no-cache ca-certificates
COPY --from=build-consumer /bin/consumer /usr/local/bin/consumer
EXPOSE 8080
ENTRYPOINT ["consumer"]

FROM alpine:3.21 AS controlplane
RUN apk add --no-cache ca-certificates docker-cli
COPY --from=build-controlplane /bin/controlplane /usr/local/bin/controlplane
EXPOSE 9090
ENTRYPOINT ["controlplane"]

# Kubernetes variant — no docker-cli, smaller image.
FROM alpine:3.21 AS controlplane-k8s
RUN apk add --no-cache ca-certificates
COPY --from=build-controlplane /bin/controlplane /usr/local/bin/controlplane
ENV BACKEND=k8s
EXPOSE 9090
ENTRYPOINT ["controlplane"]
