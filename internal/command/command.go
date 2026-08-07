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
		return Err("ERR unknown command '" + name + "'")
	}
	if len(args) < sp.minArgs || (sp.maxArgs >= 0 && len(args) > sp.maxArgs) {
		return Err("ERR wrong number of arguments for '" + name + "' command")
	}
	return sp.run(r.engine, args)
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
