package command

import (
	"slices"
	"strconv"
	"testing"
)

func bulkStrings(t *testing.T, reply Reply) []string {
	t.Helper()
	out := make([]string, 0, len(reply.Array))
	for i, item := range reply.Array {
		if item.Kind != ReplyBulk {
			t.Fatalf("array element %d = %+v, want bulk string", i, item)
		}
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
	if got.Kind != ReplyArray || !slices.Equal(bulkStrings(t, got), []string{"alpha"}) {
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
	if got.Array[1].Kind != ReplyArray || !slices.Equal(bulkStrings(t, got.Array[1]), []string{"a"}) {
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
		{"unknown numeric cursor", []string{"SCAN", "99"}, "ERR invalid cursor"},
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
	if got.Kind != ReplyArray || !slices.Equal(bulkStrings(t, got.Array[1]), []string{"beta"}) {
		t.Fatalf("last option did not win: %+v", got)
	}
}

func TestScanCursorIsOpaqueSingleUseAndMatchCannotChange(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, key := range []string{"alpha", "alpine", "atom"} {
		_ = r.Dispatch(args("SET", key, "v"))
	}

	first := r.Dispatch(args("SCAN", "0", "MATCH", "a*", "COUNT", "1"))
	if first.Kind != ReplyArray || len(first.Array) != 2 || first.Array[0].Kind != ReplyBulk {
		t.Fatalf("initial SCAN = %+v, want nested array with bulk cursor", first)
	}
	firstCursor := first.Array[0].Str
	if cursor, err := strconv.ParseUint(firstCursor, 10, 64); err != nil || cursor == 0 {
		t.Fatalf("initial cursor = %q, want nonzero unsigned decimal", firstCursor)
	}
	if got := bulkStrings(t, first.Array[1]); !slices.Equal(got, []string{"alpha"}) {
		t.Fatalf("first keys = %q, want [alpha]", got)
	}

	changed := r.Dispatch(args("SCAN", firstCursor, "MATCH", "b*", "COUNT", "1"))
	if changed.Kind != ReplyError || changed.Str != "ERR scan MATCH cannot change during iteration" {
		t.Fatalf("changed MATCH = %+v", changed)
	}

	second := r.Dispatch(args("SCAN", firstCursor, "MATCH", "a*", "COUNT", "1"))
	if second.Kind != ReplyArray || second.Array[0].Kind != ReplyBulk {
		t.Fatalf("identical MATCH after rejection = %+v", second)
	}
	secondCursor := second.Array[0].Str
	if secondCursor == "0" || secondCursor == firstCursor || !slices.Equal(bulkStrings(t, second.Array[1]), []string{"alpine"}) {
		t.Fatalf("second page = %+v, want replacement cursor and [alpine]", second)
	}
	if got := r.Dispatch(args("SCAN", firstCursor)); got.Kind != ReplyError || got.Str != "ERR invalid cursor" {
		t.Fatalf("consumed cursor = %+v, want exact invalid-cursor error", got)
	}

	last := r.Dispatch(args("SCAN", secondCursor, "COUNT", "99"))
	if last.Kind != ReplyArray || last.Array[0].Kind != ReplyBulk || last.Array[0].Str != "0" ||
		!slices.Equal(bulkStrings(t, last.Array[1]), []string{"atom"}) {
		t.Fatalf("omitted MATCH/count-changing final page = %+v", last)
	}
	if got := r.Dispatch(args("SCAN", secondCursor)); got.Kind != ReplyError || got.Str != "ERR invalid cursor" {
		t.Fatalf("completed cursor = %+v, want exact invalid-cursor error", got)
	}
}

func TestScanSessionLimitHasExactPublicError(t *testing.T) {
	r, _ := newTestRegistry(t)
	_ = r.Dispatch(args("SET", "a", "v"))
	_ = r.Dispatch(args("SET", "b", "v"))
	for i := 0; i < 16; i++ {
		got := r.Dispatch(args("SCAN", "0", "COUNT", "1"))
		if got.Kind != ReplyArray || got.Array[0].Kind != ReplyBulk || got.Array[0].Str == "0" {
			t.Fatalf("session %d = %+v, want retained cursor", i, got)
		}
	}
	got := r.Dispatch(args("SCAN", "0", "COUNT", "1"))
	if got.Kind != ReplyError || got.Str != "ERR scan session limit reached" {
		t.Fatalf("session over limit = %+v, want exact limit error", got)
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
