# Wire Protocol

go-kv-store speaks a deliberately small, RESP2-compatible subset. `redis-cli`
and RESP2 client libraries talk to it without modification. Anything not listed
here is out of scope and is not part of the contract.

## Requests

A request is a RESP2 **array of bulk strings**:

    *<count>\r\n
    $<len>\r\n<bytes>\r\n
    ... repeated <count> times

`<count>` must be at least 1. Inline commands are **not** supported: a request
that does not begin with `*` is a protocol error. RESP3 is **not** supported and
`HELLO` is not implemented, so a client that negotiates RESP3 will fall back to
RESP2 or fail to connect.

## Responses

| Type | Encoding | Used by |
|---|---|---|
| Simple String | `+OK\r\n` | `SET`, `PING` |
| Error | `-ERR ...\r\n` | any failure |
| Integer | `:1\r\n` | `DEL`, `EXISTS`, `INCR`, `DECR` |
| Bulk String | `$3\r\nfoo\r\n` | `GET` |
| Null Bulk String | `$-1\r\n` | `GET` on a missing key, and each missing key inside an `MGET` |
| Array | `*2\r\n...` | `MGET` |

Simple String and Error replies are single lines terminated by the CRLF the
encoder appends, so they carry no length prefix. Any CR or LF inside such a
reply is written as a space. This matters because error text quotes
client-supplied data: without the substitution, a command name containing CRLF
would end the frame early and every following byte would be read as an
additional reply, permanently desynchronising a pipelining client. Bulk strings
are length-prefixed and are therefore exempt — CR and LF inside a value are
payload and are returned unaltered.

## Commands

| Command | Arity | Reply |
|---|---|---|
| `PING [message]` | 1–2 | `+PONG`, or `message` as a Bulk String |
| `SET key value [EX s \| PX ms]` | ≥3 | `+OK` |
| `GET key` | 2 | Bulk, or Null Bulk if absent |
| `DEL key [key ...]` | ≥2 | Integer: keys removed |
| `EXISTS key [key ...]` | ≥2 | Integer: keys present, duplicates counted |
| `EXPIRE key seconds` | 3 | Integer: `1` applied, `0` no such key |
| `TTL key` | 2 | Integer: `-2` no key, `-1` no TTL, else seconds |
| `PERSIST key` | 2 | Integer: `1` a TTL was removed, `0` otherwise |
| `MGET key [key ...]` | ≥2 | Array, one element per key in request order |
| `INCR key` | 2 | Integer: the value after adding one |
| `DECR key` | 2 | Integer: the value after subtracting one |

Command names are case-insensitive. Keys and values are binary-safe: any byte
sequence is permitted, including NUL and CRLF.

## Expiration

A key is absent the moment its deadline passes. Reclaiming its memory is a
separate, later event, so a client can never observe when reclamation ran.

`SET` takes at most one expiry option. Repeating the same option is accepted and
the last one wins; mixing `EX` and `PX` is a syntax error. **A `SET` without an
expiry option clears any TTL the key already had** — it is not "leave the
existing expiry alone".

`TTL` reports whole seconds, rounded to nearest: `(milliseconds + 500) / 1000`.
A key set with `PX 1500` reports `1`, and one set with `PX 1600` reports `2`.

`EXPIRE` with zero or a negative value deletes the key and replies `1` if it
existed, rather than rejecting the call.

| Option error | Reply |
|---|---|
| unrecognised option | `ERR syntax error` |
| `EX` and `PX` together | `ERR syntax error` |
| option with no value | `ERR syntax error` |
| non-integer value | `ERR value is not an integer or out of range` |
| zero, negative or out-of-range value | `ERR invalid expire time in '<command>' command` |

## Counters

`INCR` and `DECR` add or subtract one and reply with the result. A key that is
missing, or expired and not yet reclaimed, counts as zero, so `INCR` on a fresh
key replies `1` and `DECR` replies `-1`. The result is stored as its decimal
text and is an ordinary string value afterwards.

**Any expiry the key already carries is preserved exactly**, not refreshed and
not dropped. This is the one behaviour of these two commands worth stating on
its own: the append-only file records `SET key <result> PXAT <the same absolute
deadline>`, so a key that survives a crash comes back with the expiry it had.

**The incrementable-value grammar is narrower than Go's `strconv.ParseInt`.** A
value is incrementable when it is `0`, or an optional `-` followed by a digit
`1`–`9` and further digits, and fits in an int64. So `+5`, `07`, `00` and `-0`
are **not** incrementable, although Go's standard library parses all four. This
matches Redis and was measured against it rather than assumed.

| Counter error | Reply |
|---|---|
| the value is not incrementable | `ERR value is not an integer or out of range` |
| the result would not fit in an int64 | `ERR increment or decrement would overflow` |

Neither error changes anything: the value and its expiry are left as they were.

## Error classes

Conformance tests compare error **class**, not exact message text.

| Class | Message | Connection |
|---|---|---|
| unknown command | `ERR unknown command '<name>'` | stays open |
| wrong arity | `ERR wrong number of arguments for '<name>' command` | stays open |
| syntax error | `ERR syntax error` | stays open |
| not an integer | `ERR value is not an integer or out of range` | stays open |
| invalid expire time | `ERR invalid expire time in '<name>' command` | stays open |
| overflow | `ERR increment or decrement would overflow` | stays open |
| shutting down | `ERR server is shutting down` | stays open |
| internal error | `ERR internal error` | stays open |
| max clients | `ERR max number of clients reached` | **closed** |
| protocol error | `ERR Protocol error: <detail>` | **closed** |

A protocol error closes the connection because there is no reliable point in the
byte stream to resume parsing from.

### Command names in error text

The two `<name>` placeholders above do not follow the same rule, and the
difference is deliberate — it is Redis's, and we match it:

- **unknown command** echoes the name exactly as the client sent it, casing
  included. There is no canonical form for a command the server does not know.
  The echoed name is truncated to 128 bytes followed by `...`; a name may
  otherwise be as long as the bulk-string limit, and reflecting one of those in
  full would let a client amplify a small request into a large reply.
- **wrong arity** reports the canonical name in lowercase. The command is known,
  so a canonical form exists, and repeating the client's casing would be noise.

Both are pinned by tests in `internal/command`.

## Deviations from Redis

- The unknown-command error omits the argument list Redis appends. Redis 8
  answers `nope foo bar` with ``ERR unknown command 'nope', with args beginning
  with: 'foo' 'bar' ``; we answer `ERR unknown command 'nope'`. When the unknown
  command carries no arguments, the two texts are identical.
- `HELLO`, `CLIENT`, `COMMAND`, `INFO`, `SELECT`, `FLUSHDB` and every other
  administrative command are unimplemented and answer with the unknown-command
  error. There is one implicit database and no way to select another.
- `SET` implements `EX` and `PX` only. `NX`, `XX`, `GET` and `KEEPTTL` are
  unimplemented and answer with `ERR syntax error`, which is also what Redis
  replies to an option it does not recognise — so the texts agree while the
  behaviour does not.
- `PTTL`, `PEXPIRE`, `EXPIREAT` and `PEXPIREAT` are not implemented.
- `MSET`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETSET` and `SETNX` are not
  implemented. `MSET` is a deliberate omission rather than a gap: it cannot be
  expressed as a single canonical append-only record, and one complete record is
  one recovery atomicity unit.
- An empty request array (`*0`) is a protocol error here and closes the
  connection. The `command` layer also carries an `ERR empty command` reply for
  a zero-argument dispatch, but the decoder rejects `*0` first, so that reply is
  a guard for direct callers of the package and is not reachable over the wire.

## Limits

| Limit | Flag | Default |
|---|---|---|
| Arguments per command | `-max-array-elements` | 1024 |
| Bulk string length | `-max-bulk-length` | 64 MiB |
| Total argument bytes per command | `-max-command-bytes` | 128 MiB |
| Concurrent clients | `-max-clients` | 1024 |
| Idle read timeout | `-timeout` | disabled |

Exceeding a size limit produces a protocol error and closes the connection.

The first two limits bound one dimension each and multiply if nothing bounds
their product: 1024 arguments of 64 MiB is a 64 GiB frame that both of them
accept. `-max-command-bytes` bounds the total, and is the limit that decides how
much one connection can make the server hold at once. It is checked against each
declared length before any payload is read, so an oversized frame is refused
without being buffered. Setting it to 0 disables the check.

Peak resident memory during a maximum-size command is roughly three times the
configured value, because the decode buffer grows by doubling and the previous
array is still live while it is copied. Budget for that when raising the flag.
Exceeding the client limit produces the max-clients error and closes the new
connection; existing ones are unaffected.

A 30-second write deadline is applied to every reply. It is not configurable in
v0.1 — it exists so that a client which stops reading cannot pin a connection
goroutine indefinitely, not as a tuning knob.
