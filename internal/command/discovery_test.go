package command

import (
	"slices"
	"testing"
)

func bulkStrings(reply Reply) []string {
	out := make([]string, 0, len(reply.Array))
	for _, item := range reply.Array {
		out = append(out, item.Str)
	}
	return out
}

func TestKeysAndDBSize(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, key := range []string{"beta", "alpha", "binary\x00\xff"} {
		if got := r.Dispatch(args("SET", key, "v")); got.Kind != ReplySimple {
			t.Fatalf("SET %q: %+v", key, got)
		}
	}
	if got := r.Dispatch(args("DBSIZE")); got.Kind != ReplyInt || got.Int != 3 {
		t.Fatalf("DBSIZE = %+v, want integer 3", got)
	}
	got := r.Dispatch(args("KEYS", "a*"))
	if got.Kind != ReplyArray || !slices.Equal(bulkStrings(got), []string{"alpha"}) {
		t.Fatalf("KEYS = %+v", got)
	}
}

func TestScanReplyShape(t *testing.T) {
	r, _ := newTestRegistry(t)
	_ = r.Dispatch(args("SET", "a", "v"))
	got := r.Dispatch(args("SCAN", "0"))
	if got.Kind != ReplyArray || len(got.Array) != 2 {
		t.Fatalf("SCAN outer reply = %+v", got)
	}
	if got.Array[0].Kind != ReplyBulk || got.Array[0].Str != "0" {
		t.Fatalf("SCAN cursor = %+v", got.Array[0])
	}
	if got.Array[1].Kind != ReplyArray || !slices.Equal(bulkStrings(got.Array[1]), []string{"a"}) {
		t.Fatalf("SCAN keys = %+v", got.Array[1])
	}
}

func TestScanOptionsAndErrors(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, key := range []string{"alpha", "alpine", "beta"} {
		_ = r.Dispatch(args("SET", key, "v"))
	}
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{"malformed cursor", []string{"SCAN", "nope"}, "ERR invalid cursor"},
		{"negative cursor", []string{"SCAN", "-1"}, "ERR invalid cursor"},
		{"overflow cursor", []string{"SCAN", "18446744073709551616"}, "ERR invalid cursor"},
		{"lone plus cursor", []string{"SCAN", "+"}, "ERR invalid cursor"},
		{"double plus cursor", []string{"SCAN", "++0"}, "ERR invalid cursor"},
		{"zero count", []string{"SCAN", "0", "COUNT", "0"}, errSyntax},
		{"negative count", []string{"SCAN", "0", "COUNT", "-1"}, errSyntax},
		{"bad count", []string{"SCAN", "0", "COUNT", "nope"}, errNotAnInteger},
		{"missing match", []string{"SCAN", "0", "MATCH"}, errSyntax},
		{"unknown option", []string{"SCAN", "0", "BOGUS", "x"}, errSyntax},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := r.Dispatch(args(tc.argv...))
			if got.Kind != ReplyError || got.Str != tc.want {
				t.Fatalf("Dispatch = %+v, want error %q", got, tc.want)
			}
		})
	}
	for _, cursor := range []string{"+0", "00"} {
		if got := r.Dispatch(args("SCAN", cursor, "MATCH", "alp*", "COUNT", "1")); got.Kind != ReplyArray {
			t.Fatalf("cursor %q rejected: %+v", cursor, got)
		}
	}
	got := r.Dispatch(args("SCAN", "0", "MATCH", "alp*", "MATCH", "beta", "COUNT", "1", "COUNT", "100"))
	if got.Kind != ReplyArray || !slices.Equal(bulkStrings(got.Array[1]), []string{"beta"}) {
		t.Fatalf("last option did not win: %+v", got)
	}
}

func TestScanCursorBeyondSnapshotCompletes(t *testing.T) {
	r, _ := newTestRegistry(t)
	got := r.Dispatch(args("SCAN", "99"))
	if got.Array[0].Str != "0" || len(got.Array[1].Array) != 0 {
		t.Fatalf("SCAN beyond snapshot = %+v", got)
	}
}

func TestDiscoveryArity(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, argv := range [][]string{{"KEYS"}, {"KEYS", "*", "extra"}, {"SCAN"}, {"DBSIZE", "extra"}} {
		if got := r.Dispatch(args(argv...)); got.Kind != ReplyError || got.Str == errSyntax {
			t.Errorf("%v = %+v, want wrong-arity error", argv, got)
		}
	}
}
