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

**Goal:** clean shutdown under load, zero race conditions, no goroutine leaks.

### What to build

- Signal handling, clean shutdown sequence
- `-race` clean on all code paths
- Benchmark with multiple concurrent clients
- Fix any goroutine leaks found

### Graceful shutdown

When `SIGINT` arrives, stop in this exact order:

```go
func (s *Server) Shutdown() {
    s.listener.Close()    // 1. stop accepting requests, wait for handlers to finish
    s.wg.Wait()           // 2. wait for all connection handlers
    s.cancel()            // 3. cancels all namespace contexts → stops TTL daemons
}
```

The connection handler loop:

```go
func (s *Server) serve() {
    for {
        conn, err := s.listener.Accept()
        if err != nil {
            return  // listener closed — clean exit
        }
        s.wg.Add(1)
        go func() {
            defer s.wg.Done()
            s.handleConn(conn)
        }()
    }
}
```

The shutdown sequence is ordered deliberately — close the listener first (stop new work entering), then drain existing connections. Reversed order would mean new connections arrive while you're trying to shut down.

### Verification checklist


| Feature           | How to verify                                                |
| ----------------- | ------------------------------------------------------------ |
| Store             | `go test -race` with concurrent readers and writers          |
| Worker pool       | Submit 200 jobs with 5 workers, verify all complete          |
| Pub/sub           | Three telnet sessions, confirm fan-out and clean unsubscribe |
| Graceful shutdown | `Ctrl+C` mid-load, verify no connections dropped mid-command |


Run `go run -race main.go` throughout. Treat every race warning as a failing test.

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
