# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o statistik-engine ./cmd/main.go

# ---
FROM alpine:3.19

RUN apk --no-cache add tzdata ca-certificates
ENV TZ=Asia/Jakarta

WORKDIR /app
COPY --from=builder /app/statistik-engine .

EXPOSE 8083

CMD ["./statistik-engine"]
