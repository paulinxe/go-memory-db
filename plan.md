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
- Each handler validates that all required keys (and other tokens) are present and writes `-ERR wrong number of arguments for X\n` if not
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
- In Phase 3, the server holds a **registry** `map[string]*store.Store` (one store per namespace name); a dedicated `Namespace` wrapper type is optional until Phase 5–6 when TTL and pub/sub attach more than a `Store` per logical database

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

## Phase 2 — Lists and hashes

**Goal:** richer value types behind the same single global store as Phase 1 — no namespaces yet.

### What to build

- **Lists:** `LPUSH`, `LPOP`, `LGET`
- **Hashes:** `HSET` (bulk: many field–value pairs in one command), `HSETONE` (one field; value may contain spaces), `HGET`, `HGETONE`, `HDEL` — `HGET` returns the whole hash (field–value pairs), symmetric with bulk `HSET`; `HGETONE` returns one field, symmetric with `HSETONE`
- Extend `Store` with `lists` and `hashes` maps; keep Phase 1 string commands as-is

### Shared rules (all new commands)

- **Parsing:** same style as Phase 1 — `strings.Fields` for the line, `strings.ToUpper` on the verb. Use `strings.Join(parts[i:], " ")` wherever a **tail** must preserve spaces (e.g. `LPUSH` element, `**HSETONE`** value). **Bulk `HSET`** does not join tails per pair: every field and every value must be a **single** token; there must be an **even** number of tokens after the key (`field1 value1 field2 value2 …`). Values containing spaces belong in `**HSETONE`**, not in bulk `HSET`. Quoted arguments (e.g. `"John Doe"`) are **out of scope** until a dedicated lexer is added.
- **Key errors:** when the line does not supply every key, field, and value the command expects → `-ERR wrong number of arguments for <COMMAND>\n` (match Phase 1 wording).
- **Unknown / missing keys:** follow Redis-ish behaviour below; use a single consistent `-ERR ...` string per case so tests and telnet sessions stay predictable.
- **Key type conflicts:** a key can be a string **or** a list **or** a hash, never two at once. **Mutations** (`SET`, `LPUSH`, `HSET`, `HSETONE`, …) must reject cross-type reuse with something like `-ERR WRONGTYPE …\n` by checking the other maps before applying the write. **Pure reads** that target a single type map do **not** cross-check the other maps and do **not** return `WRONGTYPE` when the same name exists as another type: `**LGET`** reads only `lists[key]` (empty list if no list there); `**HGET**` / `**HGETONE**` read only `hashes[key]` (empty hash / `key not found` / `field not found` as documented below — not `WRONGTYPE`).

### Protocol examples (lists and hashes)

```
→  LPUSH jobs send-email
←  +OK

→  LPUSH jobs backup-db
←  +OK

→  LGET jobs
←  *backup-db,send-email

→  LPOP jobs
←  +backup-db

→  HSET user:1 id 1 email jane@example.com
←  +OK

→  HSETONE user:1 name Jane Doe
←  +OK

→  HGET user:1
←  *id,1,email,jane@example.com,name,Jane Doe

→  HGETONE user:1 name
←  +Jane Doe

→  HDEL user:1 email
←  +OK
```

Exact formatting of `*` multi-bulk lines is your choice as long as it is documented and parseable; the examples above use comma-separated tokens in one line (same spirit as `KEYS`). If you prefer each command to require a fixed key pattern instead of free-form tails, say so in the README and tighten the grammar.

---

### `LPUSH`

- **Form:** `LPUSH <key> <element>` — one element per command for simplicity; the element is `strings.Join(parts[2:], " ")` so values can contain spaces.
- **Behaviour:** append `element` to the list stored at `key`. If `key` does not exist, create an empty list first. If `key` exists but is not a list → `WRONGTYPE`.
- **Interaction with strings/hashes:** pushing to a new key should not create a row in `strings` or `hashes`; only `lists[key]` grows. If the key was a string or hash, that is `WRONGTYPE` (unless you explicitly `DEL` first — Phase 1 `DEL` should remove the key from whichever map holds it once you track type; see note under **Store** below).
- **Response:** `+OK\n` on success.
- **Locking:** writer lock for the duration of read-modify-write on the slice.

### `LPOP`

- **Form:** `LPOP <key>` — exactly two tokens.
- **Behaviour:** remove and return the **left** (head) element — consistent with “L” in `LPUSH` / `LGET`. If the list is empty or the key does not exist → treat as “nothing to pop”: respond with `-ERR no such key\n` or `-ERR list is empty\n` (pick one and use it everywhere; Redis uses nil for empty list on `LPOP`; for this text protocol an error line is fine if documented).
- **If key is wrong type:** `WRONGTYPE`.
- **Response:** `+<element>\n` on success (element may contain spaces if you ever allow that via a different encoding; with the `Join` rule, popped values are whatever was stored).

### `LGET`

- **Form:** `LGET <key>` — exactly two tokens.
- **Semantics:** return the **entire** list stored at `key`, in order from first element to last (same order as produced by successive `LPUSH` / `LPOP` semantics you define for the list).
- **Scope:** read **only** the list map entry for `key` — do not cross-check strings or hashes. If there is no list at `key` (missing key, empty list, or the same key name exists only as a string/hash), treat as **no list elements** and use the empty-list response below — not `WRONGTYPE`.
- **If key missing or list empty:** empty list behaviour → e.g. `*\n` or `*,\n` depending on your `KEYS` convention for “no items”.
- **Response:** one `*` line listing all elements in order, e.g. comma-separated like Phase 1 `KEYS`.

### `HSET` (bulk)

- **Form:** `HSET <key> <field1> <value1> [<field2> <value2> ...]` — after the key, arguments are alternating field / value. Each field and each value is exactly **one** token from `strings.Fields` (no spaces inside the field name or inside the value for this command).
- **Arity:** at least four tokens (`HSET`, `key`, one field, one value). The number of tokens after `<key>` must be **even**; otherwise report a wrong-argument error (odd field–value tail).
- **Behaviour:** **merge** all pairs into the hash at `key` (set or overwrite those fields; leave any other fields on the hash unchanged). If the hash does not exist, create `map[string]string` for that key. If `key` exists as a string or list → `WRONGTYPE`.
- **Whitespace in values:** not supported here — use `**HSETONE`** (tail join) for values like `Jane Doe`.
- **Response:** `+OK\n` (Redis returns an integer for new vs updated fields; `+OK` is enough for the capstone unless you want that same behaviour).

### `HSETONE`

- **Form:** `HSETONE <key> <field> <value...>` — `field` is `parts[2]`; `value` is `strings.Join(parts[3:], " ")` (same tail rule as `LPUSH`).
- **Arity:** at least four tokens (`HSETONE`, `key`, `field`, and at least one word for the value); fewer → wrong number of arguments (same spirit as `SET key value`). If you later allow an empty value, document `parts[3:]` being empty for exactly three tokens after the verb.
- **Behaviour:** set a single `field` inside the hash at `key`. If the hash does not exist, create it. If `key` exists as string or list → `WRONGTYPE`.
- **Response:** `+OK\n`.

### `HGET`

- **Form:** `HGET <key>` — exactly two tokens (same arity pattern as `GET` / `LGET`: one key, whole value).
- **Scope:** read **only** `hashes[key]` — do not consult `strings` / `lists`. If there is no hash at `key` (missing entry, or the name exists only as a string/list), treat as **no hash pairs** — same empty response as below — **not** `WRONGTYPE`.
- **Behaviour:** return every field–value pair in that hash entry. Order need not be sorted; document whether iteration order is arbitrary (map order) or sorted keys for stable telnet output.
- **If key missing or empty hash:** `*` line with no pairs (same empty convention as `KEYS` / `LGET` empty list).
- **Response:** one `*` line encoding pairs — e.g. flat `field1,value1,field2,value2` as in the example above. Avoid ambiguous field values that contain commas unless you escape or switch to a counted multiline format later.

### `HGETONE`

- **Form:** `HGETONE <key> <field>` — three tokens.
- **Scope:** read **only** `hashes[key]` — do not consult `strings` / `lists` (same idea as `LGET` / `HGET`). If there is no hash at `key`, respond as **missing hash** even when the same name is a string or list — **not** `WRONGTYPE`.
- **Behaviour:** return the value for `field` in hash `key`. Missing hash → e.g. `-ERR key not found\n` (match `GET`). Missing field in an existing hash → e.g. `-ERR field not found\n` (pick one consistent scheme).
- **Response:** `+<value>\n` on success.

### `HDEL`

- **Form:** `HDEL <key> <field>` — one field per invocation for minimal scope; optional extension: multiple fields in one line if you document it.
- **Behaviour:** delete `field` from hash `key`. If hash becomes empty you may delete the hash key from `hashes` so `KEYS` / type checks stay consistent.
- **If key is not a hash:** If hash missing → `+OK\n` (nothing to delete) or a no-op error — pick one.
- **Response:** `+OK\n`.

---

### The store — `sync.RWMutex`

Same locking idea as Phase 1: many concurrent readers, writers exclusive. Add `lists` (`map[string][]string`) and `hashes` (`map[string]map[string]string`):

```go
type Store struct {
    mu      sync.RWMutex
    strings map[string]string
    lists   map[string][]string
    hashes  map[string]map[string]string
}
```

String `Get` / `Set` / `Del` stay as in Phase 1 **but** `DEL` and any “exists?” logic must be type-aware: deleting a key removes it from whichever of `strings`, `lists`, or `hashes` holds it (Phase 1 only had `strings`; extend `Del` accordingly). `SET` on an existing list/hash key should either be `WRONGTYPE` or implicitly wipe other types — **recommend `WRONGTYPE`** so data is never silently destroyed.

`**KEYS` in Phase 2:** return the union of keys present in `strings`, `lists`, and `hashes` (each key lives in at most one map, so no deduplication logic beyond scanning all three). Order can remain undefined, as in Phase 1.

List and hash handlers use `RLock` for pure reads (`HGET`, `HGETONE`, `LGET`) and `Lock` for mutations (`LPUSH`, `LPOP`, `HSET`, `HSETONE`, `HDEL`), all held for the shortest coherent section (no I/O under the lock).

**Deferred to Phase 5 (TTL):** the `expiry` map, `isExpired`, and lazy TTL checks on read. Mixing that here blurs “new value shapes” with “time-based eviction.”

`Server` is still the Phase 1 shape (`store Store` only). Namespace switching arrives next.

---

## Phase 3 — Namespaces

**Goal:** multiple clients attached to different logical databases, with a race-free registry and explicit create vs switch commands.

### What to build

- **`CNAMESPACE`** — create a new namespace (new `*store.Store` registered under a name). Serialize inserts with a **write lock** (or read-then-upgrade with a **double-check** under the write lock) so two concurrent creators for the same name cannot clobber each other.
- **`USE`** — switch this connection to an **existing** namespace only (lookup under read lock). If the name is missing, return an error — **do not** auto-create on `USE`.
- **`map[string]*store.Store`** on the server (registry), protected by `sync.RWMutex`.
- **`clientSession`** — small struct per connected client (one per `handleConnection` goroutine) holding `srv *Server` and `store *store.Store` (the active namespace’s store). Data commands use `session.store`. **`USE`** updates `session.store` when switching namespaces. **`CNAMESPACE` does not** change the active store — the client must **`USE`** to point the connection at the new namespace.
- **`DNAMESPACE`** (remove a namespace) is **Phase 4** — not part of Phase 3.

### Protocol additions

```
→  CNAMESPACE db1
←  +OK

→  USE db1
←  +OK                      ← switched to existing namespace

→  USE missing
←  -ERR namespace does not exist
```

**Create vs switch:** **`CNAMESPACE` only registers** a new name in the map (or is idempotent if it already exists — see below). It **does not** change this connection’s `session.store`. To use data in that namespace, the client must send **`USE <name>`** afterward.

### Registry and `CNAMESPACE` (double-check)

Each namespace is an isolated `Store`. Later phases add TTL and pub/sub **per logical database** — you may introduce `type Namespace struct { Store *store.Store; ... }` then; for Phase 3 the map values can stay bare `*store.Store`.

```go
type Server struct {
    mu          sync.RWMutex
    namespaces  map[string]*store.Store
}

// createNamespace registers name → new store. Idempotent: if name already exists, no-op (handler still returns +OK).
func (s *Server) createNamespace(name string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    if _, ok := s.namespaces[name]; ok {
        return nil
    }
    s.namespaces[name] = store.NewStore()
    return nil
}
```

If two goroutines race `CNAMESPACE foo`, the mutex **serializes** them: the first inserts; the second sees an existing entry and returns idempotently. The version above does **everything under `Lock`**, so no double-check is required inside `createNamespace`. 

For **`USE`**, take `RLock`, read `s.namespaces[name]`, `RUnlock`; if missing, `-ERR`. No write lock, no creation.

### Client session — `clientSession`

Reassigning the “current store” must be visible to the **next** command on the same TCP connection. Passing `*store.Store` alone into `handleCommand` by value does **not** work: assigning a new store inside the handler only updates a local copy. Use a **small struct** on the heap, one per connection:

```go
type clientSession struct {
    srv   *Server
    store *store.Store
}

func (s *Server) handleConnection(conn net.Conn) {
    s.mu.RLock()
    defaultStore := s.namespaces["default"]
    s.mu.RUnlock()

    session := &clientSession{
        srv:   s,
        store: defaultStore,
    }
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        handleCommand(conn, scanner.Text(), session)
    }
}
```

**Chosen approach:** seed each connection with **`RLock` → read `namespaces["default"]` → `RUnlock`**, as in the snippet above (same mutex as `USE` / `CNAMESPACE`). **Why not pass the map into `handleConnection` and read `mapping["default"]` without locking?** It is still the shared map; concurrent read while another goroutine writes (e.g. `CNAMESPACE`) is a **data race** in Go. Passing the map as an argument does not change that — you must hold `RLock` for the lookup either way. A helper like `storeForDefault()` is optional sugar around those three lines.

### Example — `USE my_app` (swap the active store)

Assume `my_app` already exists in `namespaces` (someone ran `CNAMESPACE my_app` earlier). The client sends `USE my_app`. The handler must **look up** that name under the registry lock, then **assign** to `session.store`. That mutates the heap-allocated `clientSession` for this goroutine only — the next `SET` / `GET` on the same TCP connection go through `my_app`’s `*store.Store`, while other connections are unaffected.

```go
// inside handleCommand(conn net.Conn, line string, session *clientSession), USE branch
case "USE":
    if len(tokens) != 2 {
        printError(conn, "wrong number of arguments for USE. Expecting name")
        return
    }
    name := tokens[1]

    session.srv.mu.RLock()
    st, ok := session.srv.namespaces[name]
    session.srv.mu.RUnlock()
    if !ok {
        printError(conn, "namespace does not exist")
        return
    }

    session.store = st // swap — same session struct, new active store
    printSuccess(conn, "OK")
```

`USE default` is the same pattern: lookup `"default"` in the map and assign its `*store.Store` to `session.store` (no special case beyond the name string).

### Example — `CNAMESPACE` (register only, no session swap)

Call `createNamespace` (idempotent under lock), which **validates** the name (non-empty, `len(name) <= 64`) and returns `-ERR` for `invalid namespace name` or `namespace name too long` as needed. Return `+OK` on success. **Do not** assign `session.store`.

```go
case "CNAMESPACE":
    if len(tokens) != 2 {
        printError(conn, "wrong number of arguments for CNAMESPACE. Expecting name")
        return
    }
    name := tokens[1]
    if err := session.srv.createNamespace(name); err != nil {
        printError(conn, err.Error())
        return
    }
    printSuccess(conn, "OK")
```

All handlers take `*clientSession`. **`USE`** sets `session.store` to the resolved `*store.Store`. **`CNAMESPACE`** never changes `session.store` — only the registry. **`SET` / `GET` / …** use `session.store` only. **PING** ignores `session.store`. Nothing on `Server` stores “current namespace” globally — only inside each session.

### What else has to change (ripple effects)

Phase 3 is not only new verbs plus a map: every code path that today assumes one global `*store.Store` must route through **`session.store`**.

#### Default namespace

- Use a fixed name (e.g. `default`) so clients and integration tests work **without** sending `USE` first.
- **Initialize it explicitly in `NewServer`:** insert `default` → `store.NewStore()` into `namespaces` before `Serve`. Each `handleConnection` seeds `clientSession.store` by taking **`RLock`**, reading `namespaces["default"]`, then **`RUnlock`** (see snippet above).
- Treat `default` like any other name at runtime (`USE default` is valid); the only special behaviour is **pre-creation** and **initial** `session.store` on connect.

#### `USE` / `CNAMESPACE` semantics

- **Arity:** `USE <name>` and `CNAMESPACE <name>` — exactly one name token; otherwise `wrong number of arguments for …` (match existing command style).
- **Name validation (only when creating a namespace, inside `CreateNamespace` / `CNAMESPACE`):**
  - **Non-empty** — empty token → `-ERR invalid namespace name`.
  - **Max length 64 bytes** — `len(name)` is UTF-8 **byte** length; reject if `len(name) > 64` with `-ERR namespace name too long`.
  - **`USE` does not re-validate** — it only looks up the registry. An overlong name that was never created yields **`-ERR namespace does not exist`** (same as any unknown name).
  - With `strings.Fields`, the name is a **single** token, so embedded spaces are impossible; no extra rule needed unless you add quoted strings later.
- **Case sensitivity:** **Case-sensitive** — `db1` and `DB1` are different namespaces.
- **`USE` when the name is missing from the registry:** `-ERR namespace does not exist` (exact message for this case).
- **`USE` idempotency:** `USE db1` twice returns `+OK` if `db1` exists.
- **`CNAMESPACE` idempotency:** if the namespace **already exists** (including **`default`**, which `NewServer` pre-inserts), return `+OK` and **do not** replace the existing `*store.Store`.

#### `Server` struct and constructor

- Remove the flat **`Store *store.Store`** field; replace with **`namespaces map[string]*store.Store`** and **`mu sync.RWMutex`**.
- **`NewServer`** registers **`default`** with a fresh `store.NewStore()` up front. Other names appear only via **`CNAMESPACE`**.
- **No `context.Context` on `Server` in Phase 3.** Introduce a root context when you add per-namespace TTL daemons (Phase 5) and wire cancellation into graceful shutdown (Phase 7).

#### Command routing refactor

- **`handleCommand(conn, line, session *clientSession)`** — thread `session` from `handleConnection`; all data commands use `session.store`.
- **`PING`** does not need the registry. Registry mutations use `session.srv` and appropriate locking.

#### Store package (`internal/store`)

- **No semantic change** to `Store`: one instance per namespace. `Del` / `WRONGTYPE` / list+hash behaviour stay as in Phase 2.

#### Tests and tooling

- **Integration tests** implicitly use **`default`** unless the test sends `USE` / `CNAMESPACE`.
- **New tests:** two connections, `CNAMESPACE a` then `USE a` / `CNAMESPACE b` then `USE b`, same key different values; concurrent `CNAMESPACE same` + `-race` (idempotent); `USE` unknown name (including an overlong name that was never created) → `namespace does not exist`; `CNAMESPACE` overlong → `namespace name too long`; duplicate `CNAMESPACE` → `+OK` without wiping data.
- **`internal/server/testutil`:** unchanged API is fine if behaviour defaults to **`default`**.

#### `cmd/server/main.go`

- Usually **unchanged** — `NewServer` + `Serve`.

#### How this sets up later phases

- **Phase 4:** `DNAMESPACE` — remove namespaces from the registry (see next section).
- **Phase 5 (TTL):** attach expiry and ticker **per namespace** (per `*Store` or per future `Namespace` struct holding that store).
- **Phase 6 (pub/sub):** **per-namespace `Broker`** matches the Phase 1 diagram; introduce a wrapper struct when you need more than `*store.Store` per map entry.

---

## Phase 4 — Namespace deletion

**Goal:** remove a logical database from the registry so its name can be reused and its `*store.Store` can be dropped when nothing references it.

### What to build

- **`DNAMESPACE <name>`** — delete the namespace entry from `namespaces` under the registry **write lock**.
- Clear rules for **edge cases** (see below); cover them with tests and `-race`.

### Protocol addition

```
→  DNAMESPACE db1
←  +OK

→  DNAMESPACE db1
←  -ERR namespace does not exist   ← already gone (same message as `USE` on a missing name)
```

### Semantics and design choices

- **`default`:** **reject deletion** (recommended) — `DNAMESPACE default` → `-ERR` so there is always at least one namespace and existing clients always have a valid bootstrap target.
- **Unknown name:** `-ERR namespace does not exist` (same exact string as `USE` when the name is not in the registry).
- **Clients still attached:** connections may still hold `session.store` pointing at the old `*store.Store` after the name is removed from the map. Decide and document one approach:
  - **Simple:** allow it — the store is orphaned but still usable until those connections `USE` something else or disconnect; the name is gone so `USE db1` fails for new lookups, but old pointers stay valid; or
  - **Stricter:** treat subsequent commands on that session as errors until `USE` / reconnect (requires detecting “store no longer registered,” e.g. generation IDs or scanning — more work).
- **Concurrent `USE` / `RLock` readers:** deleting must happen under **`Lock`**. A connection doing `USE` under `RLock` may still resolve a pointer just before delete; that is OK if you only remove the map entry and let the `*store.Store` live until the last goroutine drops it. Do **not** delete from the map while holding only `RLock`.
- **Phase 5+:** when TTL daemons or brokers exist per namespace, `DNAMESPACE` must **stop** those goroutines and release resources — sketch that when you add Phase 5–6; for Phase 4 only the map and `*store.Store` lifetime matter.

### Tests

- `DNAMESPACE` removes name; `USE` that name afterwards fails.
- `DNAMESPACE default` rejected (if you adopt that rule).
- Concurrent `DNAMESPACE` + `CNAMESPACE` / `USE` on the same or different names under `-race`.

---

## Phase 5 — TTL expiry

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

### Store changes — `expiry` and lazy checks

Add a map from key to expiry time, and clear it on `SET` when you replace a value. Reads must treat expired keys as missing:

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

`isExpired` must run under the same lock as the read of the value — if you check expiry outside the lock, another goroutine can change `expiry` between your check and your read (time-of-check/time-of-use race).

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
