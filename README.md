# GoDB (`go-memory-db`)

An in-memory key–value style database with a small custom text protocol over TCP (similar in spirit to Redis).

## What is implemented

- **TCP server** - one goroutine per accepted client, bounded concurrency via a buffered “slot” channel (when full, new connections get an error and are closed).
- **Line protocol** - one command per line; tokens are split with `strings.Fields` (whitespace-separated). Command names are case-insensitive.
- **Per-connection session** - each client has an active **Store** (namespace). Data commands run against that store only.
- **NamespaceRegistry** - maps namespace names to isolated `Store` instances; a pre-created **default** namespace exists at startup.
- **Value types** - each key is at most one of: string, list, or hash. Conflicting writes (same key in different data structures) return `-key already exists` (the server does not use a `WRONGTYPE` string).

Future work (TTL, pub/sub, graceful shutdown, sharding, etc.) is outlined in [plan.md](plan.md).

## Requirements

- **Docker** and **Docker Compose** (for the workflow below; see [docker-compose.yaml](docker-compose.yaml) and [Dockerfile](Dockerfile)).
- [Go](https://go.dev/) 1.26+ (see `go.mod`) if you run the binary on the host without containers.

## Run the server

The [Makefile](Makefile) splits work between the **host** and the **dev container**: `build` / `dev` / `down` run on the host; `**run`**, `**test`**, and `**lint**` are meant to run **inside** the container (same working directory `/app`, repo mounted as a volume).

### 1. Build and start the dev container (on the host)

From the repo root, in order:

```bash
make build
make dev
```

`make dev` runs `docker compose up` and keeps the `dev` service running (see [docker-compose.yaml](docker-compose.yaml); port **6379** is published to the host). Leave this process running and use another terminal for the next step, or start the stack detached with `docker compose up -d` if you prefer.

### 2. Start the server (inside the container)

With the stack up, run `**make run`** in the `**dev`** service:

```bash
docker compose exec dev make run
```

That uses the Makefile `**run**` recipe: `go run ./cmd/server/main.go` with `**-port 6379**` and `**-max-connections 100**`.

Other container-local targets:

```bash
docker compose exec dev make test
docker compose exec dev make lint
```

Stop and remove the stack from the **host** with `make down`.

### Run on the host without Docker

If you have Go installed locally, you can skip Docker and run from the repo root. Program defaults:


| Flag               | Default | Meaning                    |
| ------------------ | ------- | -------------------------- |
| `-port`            | `6379`  | TCP listen port            |
| `-max-connections` | `1`     | Maximum concurrent clients |


Example (same port limits as `make run` inside the container):

```bash
go run ./cmd/server -port 6379 -max-connections 100
```

## Wire format

- **Success:** `+<payload>\n` - payload is often `OK` or the returned value(s).
- **Error:** `-<message>\n` - human-readable message

Examples use `→` for client lines and `←` for server lines.

## Commands

### Control


| Command             | Arity | Behaviour                                                                                                                          |
| ------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `PING`              | none  | Reply `+PONG`                                                                                                                      |
| `CNAMESPACE <name>` | name  | Register a new namespace (idempotent if it exists). Does **not** switch the current session. Names: non-empty, max 64 UTF-8 bytes. |
| `USE <name>`        | name  | Attach this connection to an existing namespace’s store. Unknown name → `-namespace does not exist`                                |


### Strings


| Command                | Arity       | Behaviour                                                                                              |
| ---------------------- | ----------- | ------------------------------------------------------------------------------------------------------ |
| `SET <key> <value...>` | key + value | Value is the rest of the line after the key (spaces allowed). Fails if the key exists as another type. |
| `GET <key>`            | key         | `+value` or `-key not found`                                                                           |
| `DEL <key>`            | key         | Removes the key from whichever map holds it; `+OK`                                                     |
| `KEYS`                 | none        | `+` comma-separated keys from strings, lists, and hashes (order not defined)                           |


### Lists


| Command                    | Arity         | Behaviour                                                                              |
| -------------------------- | ------------- | -------------------------------------------------------------------------------------- |
| `LPUSH <key> <element...>` | key + element | Appends one element (tail joined like `SET`). Wrong type → `-key already exists`       |
| `LPOP <key>`               | key           | Removes and returns one element from the list end; empty or missing → `-list is empty` |
| `LGET <key>`               | key           | `+` comma-separated list elements in slice order (empty list → `+` with empty payload) |


### Hashes


| Command                            | Arity                                   | Behaviour                                                                                                                                |
| ---------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `HSET <key> <f1> <v1> ...`         | key + even number of field/value tokens | Bulk set; each field and value is a single token (no spaces inside values-use `HSETONE`). Odd tail → `-hset pairs must have even length` |
| `HSETONE <key> <field> <value...>` | key, field, value                       | One field; value may contain spaces                                                                                                      |
| `HGET <key>`                       | key                                     | `+` flat comma-separated `field,value,...` sorted by field name; empty → `+` empty                                                       |
| `HGETONE <key> <field>`            | key, field                              | `+value` or `-key not found` / `-field not found`                                                                                        |
| `HDEL <key> <field>`               | key, field                              | Deletes field; removes hash if empty; no hash → no-op, `+OK`                                                                             |


## Tests

With the dev container running:

```bash
docker compose exec dev make test
```

## Try it with `telnet`

With the server running (including `docker compose exec dev make run` while **6379** is published in Compose), connect from the host:

```bash
telnet localhost 6379
```

Example session (you start in namespace `default`):

```text
SET greeting hello world
GET greeting
CNAMESPACE app1
USE app1
SET x 1
GET x
```

`CNAMESPACE` only registers a name; `USE` selects it for this connection.

## Roadmap

See [plan.md](plan.md) for **Phase 4** onward: namespace deletion, TTL, pub/sub, shutdown, and optional store sharding.