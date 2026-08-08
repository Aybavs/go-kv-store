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
| Integer | `:1\r\n` | `DEL`, `EXISTS` |
| Bulk String | `$3\r\nfoo\r\n` | `GET` |
| Null Bulk String | `$-1\r\n` | `GET` on a missing key |
| Array | `*2\r\n...` | reserved for `MGET` (v0.4) |

Simple String and Error replies are single lines terminated by the CRLF the
encoder appends, so they carry no length prefix. Any CR or LF inside such a
reply is written as a space. This matters because error text quotes
client-supplied data: without the substitution, a command name containing CRLF
would end the frame early and every following byte would be read as an
additional reply, permanently desynchronising a pipelining client. Bulk strings
are length-prefixed and are therefore exempt — CR and LF inside a value are
payload and are returned unaltered.

## Commands (v0.1.0)

| Command | Arity | Reply |
|---|---|---|
| `PING` | 1 | `+PONG` |
| `SET key value` | 3 | `+OK` |
| `GET key` | 2 | Bulk, or Null Bulk if absent |
| `DEL key [key ...]` | ≥2 | Integer: keys removed |
| `EXISTS key [key ...]` | ≥2 | Integer: keys present, duplicates counted |

Command names are case-insensitive. Keys and values are binary-safe: any byte
sequence is permitted, including NUL and CRLF.

## Error classes

Conformance tests compare error **class**, not exact message text.

| Class | Message | Connection |
|---|---|---|
| unknown command | `ERR unknown command '<name>'` | stays open |
| wrong arity | `ERR wrong number of arguments for '<name>' command` | stays open |
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
- No `SET` options (`EX`, `PX`, `NX`, `XX`, `KEEPTTL`) in v0.1; `EX` and `PX`
  arrive in v0.2.
- An empty request array (`*0`) is a protocol error here and closes the
  connection. The `command` layer also carries an `ERR empty command` reply for
  a zero-argument dispatch, but the decoder rejects `*0` first, so that reply is
  a guard for direct callers of the package and is not reachable over the wire.

## Limits

| Limit | Flag | Default |
|---|---|---|
| Arguments per command | `-max-array-elements` | 1024 |
| Bulk string length | `-max-bulk-length` | 64 MiB |
| Concurrent clients | `-max-clients` | 1024 |
| Idle read timeout | `-timeout` | disabled |

Exceeding a size limit produces a protocol error and closes the connection.
Exceeding the client limit produces the max-clients error and closes the new
connection; existing ones are unaffected.

A 30-second write deadline is applied to every reply. It is not configurable in
v0.1 — it exists so that a client which stops reading cannot pin a connection
goroutine indefinitely, not as a tuning knob.
