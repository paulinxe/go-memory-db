# GoDB — In-memory database capstone project

A multi-namespace in-memory database with a custom text protocol, TTL expiry, and pub/sub. Think Redis, but yours.

---

Phases **1–4** are implemented: TCP server, line-based protocol, strings/lists/hashes, and multi-namespace support with `CNAMESPACE` / `USE`. See **README.md** for commands, wire format, and how to run the server.

---

## Phase 6 — Pub/sub

**Goal:** three `telnet` sessions — two subscribers, one publisher — messages fan out in real time.

### What to build

- `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`
- Broker with fan-out
- Slow subscriber handling
- Multi-topic subscriptions

### Protocol additions

```
→  SUBSCRIBE news
←  +subscribed to news
←  *news hello world        ← pushed later when someone publishes

→  PUBLISH news hello world
←  +1                       ← number of subscribers that received it
```

### Pub/sub — the hardest part

The broker maintains a map of topic → list of subscriber channels. When someone publishes, it fans out to all subscriber channels:

```go
type Broker struct {
    mu          sync.RWMutex
    subscribers map[string][]chan string  // topic → subscriber channels
}

func (b *Broker) Subscribe(topic string) <-chan string {
    ch := make(chan string, 10)  // buffered — slow subscriber shouldn't block publisher
    b.mu.Lock()
    defer b.mu.Unlock()
    b.subscribers[topic] = append(b.subscribers[topic], ch)
    return ch
}

func (b *Broker) Publish(topic, message string) int {
    b.mu.RLock()
    defer b.mu.RUnlock()
    subs := b.subscribers[topic]
    for _, ch := range subs {
        select {
        case ch <- message:  // non-blocking send
        default:
            // subscriber is too slow — drop the message
            // alternatively: disconnect the slow subscriber
        }
    }
    return len(subs)
}

func (b *Broker) Unsubscribe(topic string, ch <-chan string) {
    b.mu.Lock()
    defer b.mu.Unlock()
    subs := b.subscribers[topic]
    for i, sub := range subs {
        if sub == ch {
            b.subscribers[topic] = append(subs[:i], subs[i+1:]...)
            close(sub)  // signal the subscriber goroutine to exit
            return
        }
    }
}
```

The non-blocking send in `Publish` is a real design decision — you have three options for slow subscribers: drop the message, block the publisher, or disconnect the subscriber. Each has tradeoffs.

The connection handler for a subscribed client:

```go
func (c *Conn) handleSubscribe(topic string) {
    ch := c.broker.Subscribe(topic)
    defer c.broker.Unsubscribe(topic, ch)

    for msg := range ch {  // blocks until broker closes ch
        fmt.Fprintf(c.conn, "*%s %s\n", topic, msg)
    }
}
```

This goroutine is now dedicated to pushing messages — it can't handle other commands. A subscribed connection is read-only for pushes. How will you handle a client that subscribes to multiple topics?

---

## Phase 7 — Graceful shutdown and hardening

**Goal:** clean shutdown under load, `-race`-clean code paths, no goroutine leaks, and a documented shutdown order that matches how this server is actually structured (`Serve` / `serve` / `handleConnection` / `handleCommand`, plus the per-server connection semaphore).

### Already in place (starting point)

- `**internal/server/server.go`:** `Serve()` installs `signal.Notify` on `**os.Interrupt`** and `**syscall.SIGTERM**` (buffered channel; a goroutine calls `**Close()**`).
- `**Close()`:** today it clears and closes the listener and calls `**namespaces.Shutdown()`** (see `NamespaceRegistry.Shutdown` in `internal/db/namespace.go`). This will grow into the full ordered pipeline from this phase.
- **Gaps to close in this phase:** no registry of active `net.Conn` values yet, no `**sync.WaitGroup`** for in-flight commands, no `**signal.Stop**`, and `**Close()**` does not yet wrap the full shutdown sequence in `**sync.Once**` (see below — **required**).

### Design notes (from shutdown discussion)

- `**bufio.Scanner.Scan()`** does not take a `**context**`. Unblocking it means **closing the connection** (or using read deadlines). A server-wide `**context.Context`** is optional for this TCP loop: tracking conns + closing them on shutdown is enough to exit read loops.
- **In-flight work** means time spent inside `**handleCommand`** (not time blocked in `**Scan()**`). Use `**sync.WaitGroup**` (e.g. `**inflightCmdWG**`): `**Done()**` when each command returns (e.g. `defer` right after `**Add**`). `**Add(1)**` must happen together with checking `**shuttingDown**` under read lock — see `**shuttingDown` + `RWMutex**` below so there is no gap between “flag is false” and admitting a command while shutdown sets the flag.
- **Connection registry:** store each accepted conn in a **mutex-protected set or map** (add on successful accept after taking a semaphore slot, remove in the same `defer` path as today’s `**closeConnection`**). On shutdown, iterate and `**Close()**` each conn so idle handlers unblock from `**Scan()**`.
- **Order matters:** wait for in-flight **commands** before closing client conns if you want responses to finish writing; closing conns first can make `**handleCommand`** fail on `**WriteString**` mid-response.

### `shuttingDown` + `sync.RWMutex` (recommended)

**Why:** After `**inflightCmdWG.Wait()`** returns, handlers can still get another line from `**Scan()**` on conns you have not closed yet, so `**Add(1)` / `handleCommand**` could start again while you iterate the connection registry. A shutdown flag stops *new* commands on existing TCP sessions once the listener is already closed.

**Rules:**

- `**Server*`* holds a `**shuttingDown bool**` protected by `**sync.RWMutex**` (e.g. `**shutdownMutex**`).
- **Connection side** (inside the `**for scanner.Scan()`** loop, after you have a line): `**shutdownMutex.RLock()**` → if `**shuttingDown**` **{ `RUnlock()`; break the loop (optional: send an error line) }** → `**inflightCmdWG.Add(1)`** → `**RUnlock()**` → `**defer inflightCmdWG.Done()**` → `**handleCommand(...)**`.  
**Do not** hold `**RLock`** across `**handleCommand**` — writers must be able to take `**Lock()**` to set the flag, and you must not block shutdown or serialize all command work on the read lock.
- **Shutdown side:** `**shutdownMutex.Lock()`**, set `**shuttingDown = true**`, `**Unlock()**`, after the listener is closed.

**Why this is race-free:** setting `**shuttingDown`** requires `**Lock()**`, which cannot be granted until **every** `**RLock()`** is released. No handler can sit between “saw flag false” and `**inflightCmdWG.Add(1)**` while another goroutine sets the flag, so there is no time-of-check / time-of-use window.

### Target shutdown sequence (implement explicitly)

**Keep `Close()` exported.** Shutdown is not only from the OS: embedders, `**main`** with a `defer`, and `**internal/server/testutil**` (`t.Cleanup` calling `**Close()**` while `**Serve()**` runs in a goroutine) all stop the server **programmatically**. The signal goroutine should call the **same** `**Close()`** so there is one implementation.

**Require `sync.Once`:** wrap the **entire** ordered shutdown body (steps 1–5) in `**sync.Once`** inside `**Close()**`. Multiple overlapping `**Close()**` calls (e.g. test cleanup + signal, or future `**main` defer** + signal) must execute that body **exactly once** and must not race on `**Wait()`**, `**Shutdown()**`, or the connection registry.

1. **Stop accepting:** close the listener (clear the field under the existing listener mutex). Accept loop returns; no new TCP connections.
2. **Enter shutdown mode:** `**shutdownMutex.Lock()`**, set `**shuttingDown = true**`, `**shutdownMutex.Unlock()**`. Handlers still connected will stop admitting new lines as commands (see above); bytes may still arrive until conns are closed.
3. **Drain in-flight commands:** `**inflightCmdWG.Wait()`** — no goroutine is inside `**handleCommand**`.
4. **Close all tracked client connections:** unblocks `**scanner.Scan()`** on handlers that were idle waiting for lines.
5. **Stop namespace background work:** `**namespaces.Shutdown()`** (already implemented) — cancels every namespace context so TTL daemons exit.

Optional:

- **`signal.Stop(ch)`** on the notify channel after shutdown (avoid leaking the registration if the process keeps running). **Do not `close()`** the signal channel from the handler in a way that races with a second signal; stopping notification is enough for typical “exit process” servers.

### Implementation steps

Work in roughly this order so the server stays buildable and testable at each step.

1. **Add `Server` fields:** `shutdownOnce sync.Once`; `shutdownMutex sync.RWMutex` and `shuttingDown bool`; `inflightCmdWG sync.WaitGroup` for in-flight **`handleCommand`** calls; a **connection registry** (e.g. `map[*something]struct{}` keyed by conn or a generated ID) plus a **mutex** for add/remove/iterate. Keep fields alongside the existing listener `mutex` / `namespaces` / `Port` / `MaxConnections`.
2. **Connection registry lifecycle:** when `serve` successfully takes a semaphore slot and starts **`go handleConnection(...)`**, **register** that `net.Conn` under the registry mutex. In **`handleConnection`**’s **`defer`** path (inside or next to **`closeConnection`**), **unregister** the conn so every accepted client is removed **exactly once** when the handler goroutine exits.
3. **`handleConnection` loop (not `handleCommand`):** after **`scanner.Scan()`** returns `true` and you have a line, acquire **`shutdownMutex.RLock()`**. If **`shuttingDown`**, **`RUnlock()`**, optionally **`WriteString`** a clear error line, **break** out of the loop. Otherwise **`inflightCmdWG.Add(1)`**, **`RUnlock()`**, **`defer inflightCmdWG.Done()`**, then call **`handleCommand`**. Keep **`RLock`** only around the check + **`Add`** — **never** across **`handleCommand`**.
4. **Refactor `Close()` around `sync.Once`:** exported **`Close()`** should do **`s.shutdownOnce.Do(func() { ... })`** and put the **entire** ordered shutdown inside the closure. Concurrent **`Close()`** callers must become no-ops after the first run (aside from whatever minimal safe work you keep outside **`Do`**, if any).
5. **Inside the `Do` closure, match § Target shutdown sequence:** (a) under the listener **`mutex`**, swap **`s.listener`** to `nil` and **`Close()`** the listener; (b) **`shutdownMutex.Lock()`**, set **`shuttingDown = true`**, **`Unlock()`**; (c) **`inflightCmdWG.Wait()`**; (d) under the registry mutex, copy or iterate and **`Close()`** each tracked **`net.Conn`** (usually **only** **`conn.Close()`** here — let each handler’s **`defer closeConnection`** still own **`<-connections`** so you do not double-receive on the semaphore); (e) **`s.namespaces.Shutdown()`**.
6. **`Serve()` and signals:** keep **`signal.Notify`** and the goroutine that calls **`Close()`** on **`os.Interrupt`** / **`syscall.SIGTERM`**. Optionally call **`signal.Stop(ch)`** once shutdown has finished if the process continues; do not **`close()`** the notify channel in a racy way.
7. **Hardening pass:** run **`go test -race ./...`**, fix reports; add or extend a stress test / manual run with **`SIGINT`** under load; optionally add **`connWG`** (§ Optional) if you need **`Close()`** to wait until every **`handleConnection`** goroutine has returned.

### Thread-safety and idempotency

- `**sync.Once` in `Close()`:** **required** for the full shutdown pipeline (not optional). It serializes concurrent `**Close()`** callers and guarantees a single `**inflightCmdWG.Wait()**` / registry walk / `**namespaces.Shutdown()**` sequence.
- **Listener field:** keep the existing mutex discipline for `**s.listener`** read/write.
- `**connections` semaphore:** today’s cap is correct; ensure shutdown does not deadlock (closing conns eventually releases slots via your existing `**defer closeConnection`**).

### Hardening beyond shutdown

- `**go test -race ./...**` on store, registry, and any server tests; fix every report.
- **Stress / soak:** many concurrent clients (simple `go test` or a small driver) sending mixed commands; repeat with **SIGINT** / **SIGTERM** while load is on.
- **Goroutine leaks:** run tests with `**-count=1`** and consider `**go test -timeout**` plus runtime `**pprof` goroutine** profile or `**goleak`** in test `**TestMain**` if you add the dependency.
- **Benchmarks (optional for this phase):** `**go test -bench`** with concurrent clients establishes a baseline before Phase 8 sharding.

### Verification checklist


| Area              | How to verify                                                                                                                                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Store / registry  | `go test -race ./internal/db/...` with concurrent access                                                                                                                                                                |
| Server            | `go test -race ./internal/server/...` if tests exist; else `go run -race .` under load                                                                                                                                  |
| Graceful shutdown | With clients connected and commands in flight, send **SIGINT** or **SIGTERM**; also verify `**Close()`** from tests / programmatic callers is safe to call twice (`**sync.Once**`) and matches the same path as signals |
| Pub/sub (Phase 6) | Three sessions, fan-out, unsubscribe; after Phase 6, repeat shutdown test with subscribers attached                                                                                                                     |
| Race detector     | No warnings on repeated runs; treat each as a failing test                                                                                                                                                              |


Run `go run -race .` (or your entrypoint) during manual testing. Treat every race warning as a failing test.

---

## Phase 8 — Store sharding

**Goal:** replace the single `sync.RWMutex` inside each `Store` with N independent shards, each with its own mutex, and measure whether contention actually drops.

### What to build

- Fixed shard array (e.g. N=16) inside `Store`, each shard holding its own sub-map and `sync.RWMutex`
- A hash function (`fnv32(key) % N`) to route every key operation to the right shard
- Fix `KEYS` and TTL eviction — both now need to sweep all shards
- `go test -bench` before and after to confirm the improvement is real, not assumed

### Why this is worth doing

- **Lock granularity is a recurring design question** in any high-throughput backend. Sharding teaches you to reason about it concretely rather than by intuition.
- **Benchmarking discipline.** You'll need `go test -bench` with concurrent goroutines and `go tool pprof` to see mutex contention — skills that transfer directly to real production work.
- **Cross-shard atomicity is a trap.** The moment you try to do something atomic across two keys on different shards, you hit a fundamental problem. Discovering it here — at small scale — is exactly how you build intuition for why Redis Cluster has hash tags and why distributed transactions are hard.
- **It closes the loop with Redis.** Redis uses a similar fixed-slot approach (16384 hash slots). Having built something analogous yourself makes reading Redis Cluster internals feel familiar rather than opaque.

The interesting design question to sit with: `KEYS` currently locks the whole store. With sharding, you must lock and scan each shard in turn — what consistency guarantees does that give you, and does it matter?

---

## Questions to sit with

As you build, you'll hit these. Don't look up the answers immediately — sit with them first:

- Why does `Publish` need only a read lock even though it's writing to subscriber channels?
- What happens if a subscriber's goroutine panics — how do you prevent it from taking down the whole server?
- If two clients both run `CNAMESPACE db1` at the same time on a cold start, what goes wrong if creation is not fully serialized (or double-checked under the write lock)?
- A subscribed client disconnects without sending `UNSUBSCRIBE` — how do you detect this and clean up?

That last one is particularly interesting. You'll need to detect a broken TCP connection from the push goroutine and trigger cleanup. Think about how `context` and the write error from `fmt.Fprintf` can work together here.
