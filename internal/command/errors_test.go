package command

import (
	"errors"
	"strings"
	"testing"

	"github.com/aybavs/go-kv-store/internal/engine"
)

// ADR 0007 puts the error classes in the compatibility promise and leaves the
// text out. That split only works if the wording cannot drift by accident: this
// table is where it is allowed to change, so a reworded message is an edit here
// rather than a silent difference from docs/protocol.md.
//
// Every row goes through Dispatch, so the strings are what a client receives.
func TestDocumentedErrorClassesAreProduced(t *testing.T) {
	tests := []struct {
		class string
		want  string
		steps [][]string // the last one produces the error
	}{
		{
			class: "unknown command",
			want:  "ERR unknown command 'NOSUCHCOMMAND'",
			steps: [][]string{{"NOSUCHCOMMAND"}},
		},
		{
			class: "wrong arity",
			want:  "ERR wrong number of arguments for 'get' command",
			steps: [][]string{{"GET"}},
		},
		{
			class: "syntax error",
			want:  "ERR syntax error",
			steps: [][]string{{"SET", "k", "v", "NOSUCHOPTION"}},
		},
		{
			class: "not an integer",
			want:  "ERR value is not an integer or out of range",
			steps: [][]string{{"SET", "k", "abc"}, {"INCR", "k"}},
		},
		{
			class: "invalid expire time",
			want:  "ERR invalid expire time in 'set' command",
			steps: [][]string{{"SET", "k", "v", "EX", "0"}},
		},
		{
			class: "overflow",
			want:  "ERR increment or decrement would overflow",
			steps: [][]string{{"SET", "k", "9223372036854775807"}, {"INCR", "k"}},
		},
		{
			class: "invalid cursor",
			want:  "ERR invalid cursor",
			steps: [][]string{{"SCAN", "not-a-cursor"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.class, func(t *testing.T) {
			r, _ := newTestRegistry(t)
			var got Reply
			for _, step := range tc.steps {
				got = r.Dispatch(args(step...))
			}
			if got.Kind != ReplyError {
				t.Fatalf("got %+v, want an error of class %q", got, tc.class)
			}
			if got.Str != tc.want {
				t.Errorf("class %q produced %q, want %q.\n"+
					"The text is not part of the compatibility promise, but this table and "+
					"docs/protocol.md have to agree — change both or neither.",
					tc.class, got.Str, tc.want)
			}
		})
	}
}

// The remaining documented classes are produced elsewhere, and saying so is the
// point: a table that silently covered six of nine would look complete.
//
//   - "shutting down" and "internal error" come from mutationError, which needs
//     an engine in a particular state rather than a command;
//   - "max clients" is the server's, refused before a command is ever read;
//   - "protocol error" is resp's, and closes the connection.
//
// The first two are constants here, so their text is still pinned; the other two
// are pinned by tests in the packages that own them.
func TestErrorTextThatDispatchCannotReach(t *testing.T) {
	// Produced by mutationError, whose arms are exercised by the engine-state
	// tests in this package and in server.
	for _, tc := range []struct{ class, want string }{
		{"shutting down", "ERR server is shutting down"},
		{"internal error", "ERR internal error"},
		{"persistence unavailable", "ERR persistence unavailable"},
	} {
		if !strings.Contains(mutationErrorTexts(), tc.want) {
			t.Errorf("class %q text %q is no longer produced by mutationError", tc.class, tc.want)
		}
	}
}

// mutationErrorTexts returns every string mutationError can produce, by asking
// it rather than by listing them again. The nil case stands in for "an error
// mutationError does not recognise", which is what its default arm is for.
func mutationErrorTexts() string {
	var b strings.Builder
	for _, err := range []error{engine.ErrDraining, engine.ErrPersistenceUnavailable, errors.New("something else")} {
		b.WriteString(mutationError(err).Str)
		b.WriteByte('\n')
	}
	return b.String()
}
