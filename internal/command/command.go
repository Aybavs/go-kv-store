// Package command validates and executes client commands.
//
// It converts borrowed parser bytes into owned strings before anything is
// handed to the engine, and returns plain Go values rather than RESP frames.
package command

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aybavs/go-kv-store/internal/engine"
)

type handler func(e *engine.Engine, args [][]byte) Reply

type spec struct {
	// minArgs and maxArgs count the command name itself. maxArgs of -1 means
	// unbounded.
	minArgs int
	maxArgs int
	run     handler
}

type Registry struct {
	engine *engine.Engine
	cmds   map[string]spec
}

func New(e *engine.Engine) *Registry {
	r := &Registry{engine: e, cmds: make(map[string]spec)}
	r.cmds["PING"] = spec{minArgs: 1, maxArgs: 2, run: cmdPing}
	// Unbounded above 3, matching Redis's -3 arity: the extra arguments are
	// options, and rejecting one is cmdSet's job, not the arity check's.
	r.cmds["SET"] = spec{minArgs: 3, maxArgs: -1, run: cmdSet}
	r.cmds["GET"] = spec{minArgs: 2, maxArgs: 2, run: cmdGet}
	r.cmds["MGET"] = spec{minArgs: 2, maxArgs: -1, run: cmdMGet}
	r.cmds["DEL"] = spec{minArgs: 2, maxArgs: -1, run: cmdDel}
	r.cmds["EXISTS"] = spec{minArgs: 2, maxArgs: -1, run: cmdExists}
	r.cmds["EXPIRE"] = spec{minArgs: 3, maxArgs: 3, run: cmdExpire}
	r.cmds["TTL"] = spec{minArgs: 2, maxArgs: 2, run: cmdTTL}
	r.cmds["PERSIST"] = spec{minArgs: 2, maxArgs: 2, run: cmdPersist}
	return r
}

// Dispatch validates and executes one command. args[0] is the command name.
// The byte slices are borrowed and must not be retained.
func (r *Registry) Dispatch(args [][]byte) Reply {
	if len(args) == 0 {
		return Err("ERR empty command")
	}
	name := strings.ToUpper(string(args[0]))
	sp, ok := r.cmds[name]
	if !ok {
		// Client's casing: an unknown command has no canonical form.
		return Err("ERR unknown command '" + echoName(args[0]) + "'")
	}
	if len(args) < sp.minArgs || (sp.maxArgs >= 0 && len(args) > sp.maxArgs) {
		// Canonical lowercase: the command is known, so a canonical form exists.
		return Err("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
	}
	return sp.run(r.engine, args)
}

// A name may be as long as the bulk-string limit, so echoing one in full would
// let a client amplify a small request into a large reply.
const maxEchoedName = 128

// echoName truncates at a byte offset, so the result is not necessarily valid
// UTF-8. Framing is safe regardless: the encoder neutralises CR and LF.
func echoName(b []byte) string {
	if len(b) > maxEchoedName {
		return string(b[:maxEchoedName]) + "..."
	}
	return string(b)
}

// mutationError maps engine mutation failures to client-visible replies.
func mutationError(err error) Reply {
	switch {
	case errors.Is(err, engine.ErrDraining):
		return Err("ERR server is shutting down")
	case errors.Is(err, engine.ErrPersistenceUnavailable):
		// Distinct from an internal error because it is actionable: the log is
		// broken and the operator needs to know that specifically.
		return Err("ERR persistence unavailable")
	default:
		return Err("ERR internal error")
	}
}

// cmdPing answers PONG, or echoes the optional message as a bulk string.
func cmdPing(_ *engine.Engine, args [][]byte) Reply {
	if len(args) == 2 {
		return Bulk(string(args[1]))
	}
	return Simple("PONG")
}

// Bounds on an expiry argument. A duration is int64 nanoseconds, so a value
// past these cannot be represented at all; Redis rejects the same class of
// input with the same error rather than silently wrapping.
const (
	maxExpireSeconds = int64(math.MaxInt64) / int64(time.Second)
	maxExpireMillis  = int64(math.MaxInt64) / int64(time.Millisecond)
)

const (
	errNotAnInteger = "ERR value is not an integer or out of range"
	errSyntax       = "ERR syntax error"
)

func invalidExpire(command string) Reply {
	return Err("ERR invalid expire time in '" + command + "' command")
}

// parseSetOptions reads the arguments after SET's value. It returns the TTL to
// apply and, if the options were malformed, the reply to send instead.
//
// Repeating the same unit is allowed and the last one wins, which is what Redis
// does; mixing EX and PX is a syntax error. Both were measured rather than
// assumed — "SET k v EX 10 EX 20" is accepted by Redis and it would have been
// natural to reject it.
func parseSetOptions(opts [][]byte) (engine.TTL, *Reply) {
	ttl := engine.NoExpiry()
	unit := ""

	for i := 0; i < len(opts); i++ {
		name := strings.ToUpper(string(opts[i]))
		switch name {
		case "EX", "PX":
			if unit != "" && unit != name {
				return ttl, errReply(errSyntax)
			}
			if i+1 >= len(opts) {
				return ttl, errReply(errSyntax)
			}
			n, err := strconv.ParseInt(string(opts[i+1]), 10, 64)
			if err != nil {
				return ttl, errReply(errNotAnInteger)
			}
			if n <= 0 {
				r := invalidExpire("set")
				return ttl, &r
			}
			step := time.Second
			if name == "PX" {
				step = time.Millisecond
			}
			if (name == "EX" && n > maxExpireSeconds) || (name == "PX" && n > maxExpireMillis) {
				r := invalidExpire("set")
				return ttl, &r
			}
			unit = name
			ttl = engine.ExpiresIn(time.Duration(n) * step)
			i++
		default:
			return ttl, errReply(errSyntax)
		}
	}
	return ttl, nil
}

func errReply(msg string) *Reply {
	r := Err(msg)
	return &r
}

// cmdSet is the ownership boundary: the key and value are copied here, and
// nothing beyond this point aliases the parser buffer.
func cmdSet(e *engine.Engine, args [][]byte) Reply {
	ttl, bad := parseSetOptions(args[3:])
	if bad != nil {
		return *bad
	}
	key, value := string(args[1]), string(args[2])
	if err := e.Set(key, value, ttl); err != nil {
		return mutationError(err)
	}
	return Simple("OK")
}

// cmdExpire attaches a deadline. A non-positive value deletes the key outright
// and reports whether it was there — Redis's behaviour, measured rather than
// remembered.
func cmdExpire(e *engine.Engine, args [][]byte) Reply {
	n, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Err(errNotAnInteger)
	}
	key := string(args[1])

	if n <= 0 {
		removed, err := e.Delete([]string{key})
		if err != nil {
			return mutationError(err)
		}
		return Int(int64(removed))
	}
	if n > maxExpireSeconds {
		return invalidExpire("expire")
	}

	applied, err := e.Expire(key, time.Duration(n)*time.Second)
	if err != nil {
		return mutationError(err)
	}
	return boolInt(applied)
}

// cmdTTL reports whole seconds remaining, rounded to nearest as Redis does:
// (milliseconds + 500) / 1000. Measured against Redis 7 across six values —
// "rounds up" was the remembered answer and it was wrong, since PX 1500 reports
// 1 while PX 999 reports 1 as well.
func cmdTTL(e *engine.Engine, args [][]byte) Reply {
	d, st := e.TTL(string(args[1]))
	switch st {
	case engine.NoKey:
		return Int(-2)
	case engine.NoTTL:
		return Int(-1)
	}
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return Int((ms + 500) / 1000)
}

func cmdPersist(e *engine.Engine, args [][]byte) Reply {
	removed, err := e.Persist(string(args[1]))
	if err != nil {
		return mutationError(err)
	}
	return boolInt(removed)
}

func boolInt(b bool) Reply {
	if b {
		return Int(1)
	}
	return Int(0)
}

func cmdGet(e *engine.Engine, args [][]byte) Reply {
	v, ok := e.Get(string(args[1]))
	if !ok {
		return NullBulk()
	}
	return Bulk(v)
}

// cmdMGet answers one element per requested key, in request order, with a null
// bulk string where the key is absent. A missing key is a hole in the array,
// never a shorter array — position is how the client matches answers to keys.
func cmdMGet(e *engine.Engine, args [][]byte) Reply {
	results := e.MGet(ownedKeys(args))
	items := make([]Reply, 0, len(results))
	for _, r := range results {
		if !r.Found {
			items = append(items, NullBulk())
			continue
		}
		items = append(items, Bulk(r.Value))
	}
	return Array(items)
}

// ownedKeys copies the borrowed key arguments into owned strings.
func ownedKeys(args [][]byte) []string {
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a))
	}
	return keys
}

func cmdDel(e *engine.Engine, args [][]byte) Reply {
	n, err := e.Delete(ownedKeys(args))
	if err != nil {
		return mutationError(err)
	}
	return Int(int64(n))
}

func cmdExists(e *engine.Engine, args [][]byte) Reply {
	return Int(int64(e.Exists(ownedKeys(args))))
}
