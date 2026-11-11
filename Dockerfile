FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -trimpath e -ldflags "-s -w": build reprodutível e binário menor (sem
# caminhos da máquina, tabela de símbolos nem debug info)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o api ./cmd/api

FROM alpine:3.21
# ca-certificates: TLS de saída (SQS, DynamoDB, New Relic)
RUN apk add --no-cache ca-certificates \
  && addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --chown=app:app --from=builder /app/api ./api
USER app
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/health/live || exit 1
ENTRYPOINT ["./api"]
