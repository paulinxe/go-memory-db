# GoDB — In-memory database capstone project

A multi-namespace in-memory database with a custom text protocol, TTL expiry, and pub/sub. Think Redis, but yours.

---

## Phase 1 — TCP server and core strings

**Goal:** `telnet localhost 6379` and run commands manually.

### What to build
- TCP listener, connection handler goroutine per client
- Parse commands, route to handlers
- `SET`, `GET`, `DEL`, `KEYS`, `PING`
- Single namespace, no TTL yet

### The protocol

Simple line-based text protocol over TCP. Commands are space-separated, responses are prefixed by type:

```
→  SET mykey myvalue
←  +OK

→  GET mykey
←  +myvalue

→  DEL mykey
←  +OK

→  KEYS
←  *key1,key2,key3

→  PING
←  +PONG
```

Response prefixes:
- `+` success
- `-` error
- `*` push message (pub/sub) or multi-value response

Test everything with `telnet localhost 6379` or `nc`.

### Architecture

```
TCP listener
     │
     ▼
Connection handler (one goroutine per client)
     │
     ├── parses commands
     ├── routes to correct namespace
     └── writes responses
          │
          ▼
     Namespace
          │
          ├── Store (RWMutex — strings, lists, hashes)
          ├── TTL daemon (ticker goroutine per namespace)
          └── PubSub broker (channel-based fan-out)
```

### TCP listener — implementation notes

`net.Listen("tcp", ":6379")` binds the port and returns a `net.Listener`. It does NOT accept connections — that happens in the accept loop via `listener.Accept()`, which blocks until a client connects and returns a `net.Conn`.

The accept loop runs on the main goroutine; every connection is handed off immediately:

```go
for {
    conn, err := s.listener.Accept()
    if err != nil {
        return  // listener was closed — clean exit
    }
    select {
    case s.sem <- struct{}{}:  // acquire semaphore slot
        go func() {
            defer func() { <-s.sem }()  // release slot when done
            s.handleConn(conn)
        }()
    default:
        // at capacity — reject immediately
        conn.Write([]byte("-ERR max clients reached\n"))
        conn.Close()
    }
}
```

### Connection limit — semaphore channel

Max concurrent connections is capped at **100** using a buffered channel as a semaphore. When all slots are taken the server writes `-ERR max clients reached\n` and closes the connection immediately (same behaviour as Redis).

```go
const maxConns = 100

type Server struct {
    listener net.Listener
    sem      chan struct{}  // capacity = maxConns
}

// initialise with: sem: make(chan struct{}, maxConns)
```

### Reading the protocol

`net.Conn` is a raw byte stream. Wrap it in a `bufio.Scanner` to get complete lines:

```go
scanner := bufio.NewScanner(conn)
for scanner.Scan() {
    line := scanner.Text()
    // parse + route
}
// scanner.Scan() returns false when the client disconnects
```

### Command parsing

`strings.Fields` splits on any whitespace and handles multiple/leading/trailing spaces correctly. No regex needed.

```go
parts := strings.Fields(line)
if len(parts) == 0 {
    continue  // blank line — ignore
}

switch strings.ToUpper(parts[0]) {
case "PING":
    // no args
case "GET":
    // expects parts[1]
case "SET":
    // expects parts[1] and parts[2:]
    // value = strings.Join(parts[2:], " ")  ← supports spaces in values
case "DEL":
    // expects parts[1]
case "KEYS":
    // no args
default:
    fmt.Fprintf(conn, "-ERR unknown command\n")
}
```

Key decisions:
- `strings.ToUpper` on the verb — `set`, `SET`, and `Set` all work
- Each handler validates argument count and writes `-ERR wrong number of arguments for X\n` if wrong
- `SET` values support spaces: `SET mykey hello world` stores `"hello world"` via `strings.Join(parts[2:], " ")`

### The store — Phase 1

Phase 1 only needs strings. Start minimal — lists and hashes are added in Phase 2:

```go
type Store struct {
    mu      sync.RWMutex
    strings map[string]string
}
```

`sync.RWMutex` allows multiple concurrent readers (`RLock`) but only one writer at a time (`Lock`). Multiple clients doing `GET` simultaneously is safe and should not be serialized — only `SET` and `DEL` need exclusivity.

The four methods needed for Phase 1:

```go
func (s *Store) Get(key string) (string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    v, ok := s.strings[key]
    return v, ok
}

func (s *Store) Set(key, value string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.strings[key] = value
}

func (s *Store) Del(key string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.strings, key)
}

func (s *Store) Keys() []string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    keys := make([]string, 0, len(s.strings))
    for k := range s.strings {
        keys = append(keys, k)
    }
    return keys  // copy made under the lock — safe to use after unlock
}
```

Notes:
- `defer` on unlock is idiomatic — ensures the lock is always released even on panic
- `Keys` copies the slice under the lock; map iteration order is random, sort after the lock if needed
- In Phase 2, `Store` moves inside a `Namespace` struct — designing it as its own type now makes that refactor trivial

The `Server` struct for Phase 1:

```go
type Server struct {
    listener net.Listener
    sem      chan struct{}  // semaphore, capacity = maxConns
    store    Store
}
```

### `handleConn`

Owns a single client connection for its entire lifetime: reads lines, parses, calls the store, writes responses.

```go
func (s *Server) handleConn(conn net.Conn) {
    defer conn.Close()

    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        parts := strings.Fields(scanner.Text())
        if len(parts) == 0 {
            continue
        }

        switch strings.ToUpper(parts[0]) {
        case "PING":
            fmt.Fprintf(conn, "+PONG\n")

        case "SET":
            if len(parts) < 3 {
                fmt.Fprintf(conn, "-ERR wrong number of arguments for SET\n")
                continue
            }
            s.store.Set(parts[1], strings.Join(parts[2:], " "))
            fmt.Fprintf(conn, "+OK\n")

        case "GET":
            if len(parts) != 2 {
                fmt.Fprintf(conn, "-ERR wrong number of arguments for GET\n")
                continue
            }
            v, ok := s.store.Get(parts[1])
            if !ok {
                fmt.Fprintf(conn, "-ERR key not found\n")
            } else {
                fmt.Fprintf(conn, "+%s\n", v)
            }

        case "DEL":
            if len(parts) != 2 {
                fmt.Fprintf(conn, "-ERR wrong number of arguments for DEL\n")
                continue
            }
            s.store.Del(parts[1])
            fmt.Fprintf(conn, "+OK\n")

        case "KEYS":
            keys := s.store.Keys()
            fmt.Fprintf(conn, "*%s\n", strings.Join(keys, ","))

        default:
            fmt.Fprintf(conn, "-ERR unknown command '%s'\n", parts[0])
        }
    }
    // scanner.Scan() = false: client disconnected or read error
    // defer conn.Close() handles cleanup
}
```

Notes:
- `defer conn.Close()` at the top ensures the connection is always closed however the function exits
- `fmt.Fprintf` writes directly to `net.Conn` (implements `io.Writer`) — no write-side buffering needed
- Write errors are not checked per command; the next `scanner.Scan()` will return false on a dead connection and exit cleanly
- `KEYS` with no keys returns `*\n` (empty join) — acceptable for Phase 1

---

## Phase 2 — Lists, hashes, namespaces

**Goal:** multiple clients in different namespaces with no races.

### What to build
- `LPUSH`, `LPOP`, `LRANGE`
- `HSET`, `HGET`, `HDEL`, `HGETALL`
- `USE` command — namespace switching
- Double-check pattern in `getOrCreate`

### Protocol additions

```
→  HSET user:1 name John
←  +OK

→  HGET user:1 name
←  +John

→  USE db1
←  +OK                      ← switched namespace
```

### The store — `sync.RWMutex`

The heart of the database. Multiple clients reading simultaneously is fine — only writes need exclusivity:

```go
type Store struct {
    mu      sync.RWMutex
    strings map[string]string
    lists   map[string][]string
    hashes  map[string]map[string]string
    expiry  map[string]time.Time  // key → expiry time
}

func (s *Store) Get(key string) (string, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if s.isExpired(key) {  // check TTL under the read lock
        return "", false
    }
    v, ok := s.strings[key]
    return v, ok
}

func (s *Store) Set(key, value string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.strings[key] = value
    delete(s.expiry, key)  // clear any existing TTL
}
```

The subtle problem: `isExpired` needs to be called under a lock — if you check expiry outside the lock, another goroutine can modify `expiry` between your check and your read. This is a classic time-of-check/time-of-use race.

### Namespace switching

Each namespace is an isolated `Store` + `Broker` + TTL daemon. The namespace registry itself needs protection:

```go
type Server struct {
    mu         sync.RWMutex
    namespaces map[string]*Namespace
    ctx        context.Context
    cancel     context.CancelFunc
}

func (s *Server) getOrCreate(name string) *Namespace {
    s.mu.RLock()
    ns, ok := s.namespaces[name]
    s.mu.RUnlock()
    if ok {
        return ns  // fast path — no write lock needed
    }

    s.mu.Lock()
    defer s.mu.Unlock()
    // double-check after acquiring write lock
    if ns, ok = s.namespaces[name]; ok {
        return ns  // another goroutine created it while we waited
    }
    ns = newNamespace(s.ctx)
    s.namespaces[name] = ns
    return ns
}
```

The double-check pattern is important — between releasing the read lock and acquiring the write lock, another goroutine may have created the namespace. Without the second check you'd overwrite it.

---

## Phase 3 — TTL expiry

**Goal:** `SET foo bar` → `EXPIRE foo 5` → wait 6 seconds → `GET foo` returns nothing.

### What to build
- `EXPIRE`, `TTL` commands
- Ticker daemon per namespace
- Lazy expiry on read + active eviction on tick

### Protocol additions

```
→  EXPIRE mykey 30
←  +OK
```

### TTL expiry — ticker goroutine

A background goroutine per namespace that wakes up periodically and evicts expired keys:

```go
func (s *Store) startExpiryDaemon(ctx context.Context) {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()
    go func() {
        for {
            select {
            case <-ticker.C:
                s.evictExpired()
            case <-ctx.Done():
                return  // namespace is shutting down
            }
        }
    }()
}

func (s *Store) evictExpired() {
    s.mu.Lock()
    defer s.mu.Unlock()
    now := time.Now()
    for key, exp := range s.expiry {
        if now.After(exp) {
            delete(s.strings, key)
            delete(s.lists, key)
            delete(s.hashes, key)
            delete(s.expiry, key)
        }
    }
}
```

Worth thinking about: why does `evictExpired` need a write lock even though it's just "checking" expiry?

---

## Phase 4 — Pub/sub

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

## Phase 5 — Graceful shutdown and hardening

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

| Feature | How to verify |
|---|---|
| Store | `go test -race` with concurrent readers and writers |
| Worker pool | Submit 200 jobs with 5 workers, verify all complete |
| Pub/sub | Three telnet sessions, confirm fan-out and clean unsubscribe |
| Graceful shutdown | `Ctrl+C` mid-load, verify no connections dropped mid-command |

Run `go run -race main.go` throughout. Treat every race warning as a failing test.

---

## Questions to sit with

As you build, you'll hit these. Don't look up the answers immediately — sit with them first:

- Why does `Publish` need only a read lock even though it's writing to subscriber channels?
- What happens if a subscriber's goroutine panics — how do you prevent it from taking down the whole server?
- If two clients both `USE db1` simultaneously on a cold start, what goes wrong without the double-check?
- A subscribed client disconnects without sending `UNSUBSCRIBE` — how do you detect this and clean up?

That last one is particularly interesting. You'll need to detect a broken TCP connection from the push goroutine and trigger cleanup. Think about how `context` and the write error from `fmt.Fprintf` can work together here.
