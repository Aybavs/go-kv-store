# Writing a client

What you need to talk to this server, in the order you will need it. Every byte
sequence below was captured from a running server, not written from memory.

[docs/protocol.md](protocol.md) is the reference: every command, every error
class, every limit. This page is the narrative, and it starts with the
boundaries that decide what your application can be.

## Start here: what this server cannot do

These are not gaps to work around. They are the scope, and they shape a user
interface more than anything else here.

**Key discovery is bounded and pull-based.** Use `SCAN` for a browser and
`DBSIZE` for the logically live count. `KEYS` exists for debugging and small
datasets, but it walks the whole keyspace in one call. There is no `RANDOMKEY`.

**There is no change notification.** No Pub/Sub, no keyspace notifications, no
`MONITOR`. A completed `SCAN` is one point-in-time list, not a live view. Offer
an explicit refresh and, if the interface needs to stay current, poll at a rate
you can defend; there is nothing to subscribe to.

**There is no server introspection.** No `INFO`, no `COMMAND`, no `CLIENT`. You
cannot build a statistics panel, and you cannot ask the server what it supports —
this document and `protocol.md` are that answer.

**Everything is a string.** No lists, hashes or sets, and no `TYPE`, because
there is only one type. Values are arbitrary bytes.

**Expiry is whole seconds on read.** `TTL` returns seconds; `PTTL` does not
exist. A countdown can be no finer than that.

## Connecting

Plain TCP. Default port **6380**, configurable with `-port`.

No TLS, no `AUTH`, no `SELECT` — one implicit database, and anyone who can reach
the port has full access.

**Do not send a handshake.** `HELLO`, `CLIENT SETINFO` and `COMMAND DOCS` are
unimplemented and answer with an unknown-command error. Most Redis client
libraries send at least one of these on connect, which is the first thing that
breaks when you point one at this server. A library that can be told to skip the
handshake works; otherwise speak RESP2 directly, which is what the rest of this
page is for.

By default a connection is never closed for being idle (`-timeout` is disabled).

## Sending a command

A request is a RESP2 array of bulk strings. Nothing else — an inline command is a
protocol error.

    *<count>\r\n
    $<len>\r\n<bytes>\r\n      × count

`SET language go` on the wire:

    *3\r\n$3\r\nSET\r\n$8\r\nlanguage\r\n$2\r\ngo\r\n

`<len>` counts **bytes**, not characters. Keys and values are binary-safe: NUL
and CRLF inside them are payload and come back unaltered. In Swift that means
`Data`, not `String` — a value written by another client need not be valid
UTF-8, and decoding it as UTF-8 would replace bytes you were asked to store.

Command names are case-insensitive.

## Reading a reply

Five shapes, and the first byte tells you which:

| First byte | Shape | Example |
|---|---|---|
| `+` | simple string, ends at CRLF | `+OK\r\n` |
| `-` | error, ends at CRLF | `-ERR unknown command 'BOGUS'\r\n` |
| `:` | integer, ends at CRLF | `:1\r\n`, `:-1\r\n` |
| `$` | bulk string, length-prefixed | `$2\r\ngo\r\n`, empty is `$0\r\n\r\n` |
| `$-1` | null bulk — absent, distinct from empty | `$-1\r\n` |
| `*` | array, then that many replies | `*2\r\n$2\r\ngo\r\n$-1\r\n` |

Captured from the server:

    GET language        → $2\r\ngo\r\n
    GET nope            → $-1\r\n
    MGET language nope  → *2\r\n$2\r\ngo\r\n$-1\r\n
    INCR counter        → :1\r\n
    TTL language        → :-1\r\n

Two rules that save trouble:

- **A null bulk is not an empty string.** `GET` on a missing key gives `$-1`;
  `GET` on a key holding `""` gives `$0\r\n\r\n`. Model it as an optional.
- **Reply parsing must be recursive.** `MGET` is a flat array of bulk strings or
  nulls, but `SCAN` returns an array containing a bulk cursor and another array.
  Do not special-case array elements as bulk strings.

In Swift, keep binary payloads as `Data` and make the reply model recursive:

```swift
indirect enum RESPValue {
    case simple(String)
    case error(String)
    case integer(Int64)
    case bulk(Data?)
    case array([RESPValue])
}
```

## Browsing keys with SCAN

Start with `SCAN 0`. The response is always a two-element array: a decimal
cursor bulk string and an array of key-name bulk strings. Pass the returned
cursor to the next call until the returned cursor is `0`.

The cursor is opaque and single-use. A successful nonterminal page replaces
it, so persist only the most recently returned token and never compare, order,
increment, or reuse cursor values. Malformed, negative, overflowing, unknown,
expired, completed, consumed, and pre-restart cursors all return
`ERR invalid cursor`; restart the traversal from `0` when that happens.

`MATCH pattern` is fixed on the first call. Later calls may omit it or repeat
the exact same bytes; changing it returns
`ERR scan MATCH cannot change during iteration` without consuming the current
cursor. `COUNT` defaults to 10 and may change on every call. It controls page
size, not the cost of creating the snapshot: the initial call still copies all
live names, filters them, and sorts the retained names. Continuations do none of
that work again.

A session expires after 30 seconds without a successful continuation. Across
all clients the server keeps at most 16 unfinished traversals and a conservative
128 MiB retained-memory estimate. A new traversal that would cross either bound
gets `ERR scan session limit reached`; wait for an abandoned traversal to
expire, finish one already in progress, or retry later. These limits are not
client-configurable.

The snapshot contains names, not values. A key created after the initial call
will not appear. A returned name may have been deleted or expired before the
browser issues `GET`, so treat a null result as normal and remove or mark that
row. A value may also change while its name remains listed. If every replacement
cursor is followed to `0`, every name matching at capture is returned exactly
once. Clients must not depend on the current bytewise result order.

For each refresh, keep the traversal's pages together as one generation: begin
at `0`, accumulate until `0` returns, then replace the displayed list. Starting
a new traversal is the only way to see external key additions or removals.

## Errors are values, not failures

An error reply leaves the connection usable. Sending an unknown command and then
a `PING` on the same connection:

    -ERR unknown command 'BOGUS'\r\n+PONG\r\n

So an error belongs in your result type, not in a reconnect path.

**Two classes are different and do close the connection:** a protocol error, and
the max-clients rejection. `protocol.md` marks them in its error table. A
protocol error means the byte stream cannot be resynchronised — there is no
reliable place to resume parsing — so the server replies once and closes:

    (sending "PING\r\n" as an inline command)
    -ERR Protocol error: expected '*' at start of request\r\n   then close

**Branch on the class, not the text.** `protocol.md` enumerates the classes and
that enumeration is the contract; the wording is free to change within a major
version. Match on a prefix like `ERR unknown command` or on the documented set,
never on the whole string.

## Pipelining

You may send several commands before reading any reply. Replies come back in the
order the commands were sent, one per command. Three commands in one packet:

    *1\r\n$4\r\nPING\r\n  *2\r\n$3\r\nGET\r\n$8\r\nlanguage\r\n  *1\r\n$4\r\nPING\r\n
    → +PONG\r\n$2\r\ngo\r\n+PONG\r\n

This is also much faster: the server flushes once per batch rather than once per
reply.

**RESP2 has no request identifiers.** Replies are matched to commands by
position and nothing else. If several concurrent tasks share one connection and
each writes when it likes, the replies will be handed to the wrong callers. Two
safe designs:

- one connection per concurrent task; or
- one connection behind a serial queue — write the request and read its reply
  before the next request is written.

For a user interface, one connection behind an actor is usually enough.

## Limits and deadlines

| | Default | On breach |
|---|---|---|
| Arguments per command | 1024 | protocol error, connection closed |
| Bulk string length | 64 MiB | protocol error, connection closed |
| Total bytes per command | 128 MiB | protocol error, connection closed |
| Concurrent clients | 1024 | max-clients error, new connection closed |

Every reply carries a 30-second write deadline. A client that stops reading will
have its connection closed rather than pinning the server.

On shutdown, what a client usually observes is **the connection closing**, not
an error: the server stops admitting work and closes connections once in-flight
commands finish. Measured — a `SET` sent during shutdown got no reply at all.

`ERR server is shutting down` exists for the narrow window where a command is
already being dispatched when the drain begins, so handle it, but do not expect
it as the normal signal. Treat a closed connection as the ordinary end.

## A whole session

Bytes exactly as they went over the socket:

    → *3\r\n$3\r\nSET\r\n$8\r\nlanguage\r\n$2\r\ngo\r\n
    ← +OK\r\n

    → *2\r\n$3\r\nGET\r\n$8\r\nlanguage\r\n
    ← $2\r\ngo\r\n

    → *3\r\n$4\r\nMGET\r\n$8\r\nlanguage\r\n$4\r\nnope\r\n
    ← *2\r\n$2\r\ngo\r\n$-1\r\n

    → *2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n
    ← :1\r\n

    → *1\r\n$5\r\nBOGUS\r\n
    ← -ERR unknown command 'BOGUS'\r\n      (connection still usable)

## Semantics worth knowing before you design screens

Full detail in `protocol.md`; these are the ones that change a user interface.

- **`SET` without `EX`/`PX` clears an existing expiry.** An edit form that
  re-sends a value silently removes its TTL unless you re-send the option too.
- **`EXPIRE key 0` deletes the key** and replies `1`. It is not an error and not
  a no-op.
- **`INCR`/`DECR` keep an existing expiry**, and create a key at `1`/`-1` if it
  is absent.
- **`INCR` accepts a narrower number format than most languages parse**: `0`, or
  an optional `-` then a digit `1`–`9` and further digits. `+5`, `07`, `00` and
  `-0` are rejected with `ERR value is not an integer or out of range`. Do not
  pre-validate with Swift's `Int(_:)` and assume agreement.
- **`TTL`** returns `-2` for a missing key and `-1` for a key with no expiry.
  Both are ordinary answers, not errors.
- **`EXISTS` counts duplicates**: `EXISTS k k` is `2`.
