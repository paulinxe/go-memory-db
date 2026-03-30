# Local development via Docker Compose (see compose.yaml).
# Ctrl+C stops the stack; the container is kept. Run `make down` to remove it.
.PHONY: build dev run down test lint

# To be run from the host machine.
build:
	DOCKER_UID=$(shell id -u) DOCKER_GID=$(shell id -g) docker compose build

dev:
	DOCKER_UID=$(shell id -u) DOCKER_GID=$(shell id -g) docker compose up

down:
	docker compose down

# To be run from the container.
run:
	go run ./cmd/server/main.go -port 6379 -max-connections 100

test:
	go test -v -race ./...

lint:
	golangci-lint run
