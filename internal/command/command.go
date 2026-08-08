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
	r.cmds["PING"] = spec{minArgs: 1, maxArgs: 1, run: cmdPing}
	r.cmds["SET"] = spec{minArgs: 3, maxArgs: 3, run: cmdSet}
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
		// Redis echoes the name exactly as the client sent it here, casing and
		// all, because there is no canonical form for a command it does not
		// know. We match that.
		return Err("ERR unknown command '" + echoName(args[0]) + "'")
	}
	if len(args) < sp.minArgs || (sp.maxArgs >= 0 && len(args) > sp.maxArgs) {
		// The command is known, so there is a canonical form. Redis uses it,
		// lowercased, rather than repeating the client's casing.
		return Err("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
	}
	return sp.run(r.engine, args)
}

// maxEchoedName bounds how much of an unknown command name is quoted back to
// the client. A name may be as long as the configured bulk-string limit — 64
// MiB by default — and reflecting one of those in full would let a client
// amplify a single request into a reply of its own choosing. Redis bounds the
// same text for the same reason.
const maxEchoedName = 128

// echoName renders a client-supplied command name for an error reply. The
// result is a bounded byte string, not necessarily valid UTF-8: truncation is
// applied at a byte offset and the name itself may be arbitrary binary. The
// encoder neutralises CR and LF, so no content here can affect framing.
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

func cmdPing(_ *engine.Engine, _ [][]byte) Reply { return Simple("PONG") }

// cmdSet converts the borrowed key and value bytes into owned strings. This is
// the ownership boundary: nothing beyond this point aliases the parser buffer.
func cmdSet(e *engine.Engine, args [][]byte) Reply {
	key, value := string(args[1]), string(args[2])
	if err := e.Set(key, value); err != nil {
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
