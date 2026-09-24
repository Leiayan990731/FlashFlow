# syntax=docker/dockerfile:1.7
FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/relay ./cmd/relay && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/order-worker ./cmd/order-worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrator ./cmd/migrator

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -g 65532 app && adduser -D -u 65532 -G app app
WORKDIR /app
COPY --from=build /out/ /app/
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/app/gateway"]
