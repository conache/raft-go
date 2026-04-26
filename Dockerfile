# syntax=docker/dockerfile:1.6

FROM golang:1.24-alpine AS builder
WORKDIR /src

# Cache deps
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/raftd ./cmd/raftd \
 && CGO_ENABLED=0 go build -o /out/raftcli ./cmd/raftcli

FROM alpine:3.20
RUN adduser -D -u 1000 raft
COPY --from=builder /out/raftd  /usr/local/bin/raftd
COPY --from=builder /out/raftcli /usr/local/bin/raftcli
USER raft
ENTRYPOINT ["raftd"]
