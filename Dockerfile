# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS builder

ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o statistik-engine ./cmd/main.go

# ---
FROM alpine:3.19

RUN apk --no-cache add tzdata ca-certificates
ENV TZ=Asia/Jakarta

WORKDIR /app
COPY --from=builder --chmod=755 /app/statistik-engine .

EXPOSE 8083

CMD ["./statistik-engine"]
