.PHONY: fmt test race vet build docker-up docker-down smoke load

fmt:
	gofmt -w $$(find cmd internal -name '*.go')

test:
	go test ./...

race:
	CGO_ENABLED=1 go test -race ./...

vet:
	go vet ./...

build:
	go build ./cmd/gateway ./cmd/relay ./cmd/order-worker ./cmd/migrator

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

smoke:
	pwsh -File scripts/smoke.ps1

load:
	k6 run scripts/load.js
