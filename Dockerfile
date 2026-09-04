# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/etl ./cmd/etl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=build /out/etl /usr/local/bin/etl
RUN mkdir -p /app/data/raw /app/data/processed /app/logs
ENV HTTP_ADDR=:8080 \
    DATA_DIR=/app/data \
    LOG_PATH=/app/logs/etl.log \
    POLL_INTERVAL=30s
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/etl"]
