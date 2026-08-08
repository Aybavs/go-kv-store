// Package command validates and executes client commands.
//
// It converts borrowed parser bytes into owned strings before anything is
// handed to the engine, and returns plain Go values rather than RESP frames.
package command

import (
	"errors"
	"strings"

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
	r.cmds["DEL"] = spec{minArgs: 2, maxArgs: -1, run: cmdDel}
	r.cmds["EXISTS"] = spec{minArgs: 2, maxArgs: -1, run: cmdExists}
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
	if errors.Is(err, engine.ErrDraining) {
		return Err("ERR server is shutting down")
	}
	return Err("ERR internal error")
}

// cmdPing answers PONG, or echoes the optional message as a bulk string.
func cmdPing(_ *engine.Engine, args [][]byte) Reply {
	if len(args) == 2 {
		return Bulk(string(args[1]))
	}
	return Simple("PONG")
}

// cmdSet is the ownership boundary: the key and value are copied here, and
// nothing beyond this point aliases the parser buffer.
//
// Options are a syntax error rather than a wrong-arity error — the count is
// legal, the option is not. v0.2 replaces this branch with the EX/PX parser.
func cmdSet(e *engine.Engine, args [][]byte) Reply {
	if len(args) > 3 {
		return Err("ERR syntax error")
	}
	key, value := string(args[1]), string(args[2])
	if err := e.Set(key, value, engine.NoExpiry()); err != nil {
		return mutationError(err)
	}
	return Simple("OK")
}

func cmdGet(e *engine.Engine, args [][]byte) Reply {
	v, ok := e.Get(string(args[1]))
	if !ok {
		return NullBulk()
	}
	return Bulk(v)
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
