# Local development via Docker Compose (see compose.yaml).
# Ctrl+C stops the stack; the container is kept. Run `make down` to remove it.
.PHONY: build run down test lint

build:
	DOCKER_UID=$(shell id -u) DOCKER_GID=$(shell id -g) docker compose build

run:
	DOCKER_UID=$(shell id -u) DOCKER_GID=$(shell id -g) docker compose up

down:
	docker compose down

test:
	go test ./...

lint:
	golangci-lint run
