package glob

import "testing"

func TestMatch(t *testing.T) {
	tests := []struct {
		name, pattern, value string
		want                 bool
	}{
		{"exact", "alpha", "alpha", true},
		{"exact miss", "alpha", "alph", false},
		{"star empty", "a*", "a", true},
		{"star many", "a*b*c", "axybzzc", true},
		{"collapsed stars", "a**c", "abc", true},
		{"question byte", "a?c", "a\xffc", true},
		{"question is not rune", "?", "é", false},
		{"two questions match utf8 bytes", "??", "é", true},
		{"class", "class[ab]", "classb", true},
		{"class miss", "class[ab]", "classc", false},
		{"range", "[a-c]", "b", true},
		{"reversed range", "[c-a]", "b", true},
		{"signed range excludes positive byte", "[\x00-\xff]", "\x01", false},
		{"signed range includes zero endpoint", "[\x00-\xff]", "\x00", true},
		{"signed range includes negative endpoint", "[\x00-\xff]", "\xff", true},
		{"signed negative range", "[\x80-\xff]", "\xfe", true},
		{"signed range crosses byte sign", "[\x7f-\x80]", "\x01", true},
		{"negated class", "[^ab]", "c", true},
		{"negated class miss", "[^ab]", "a", false},
		{"escaped star", "literal\\*", "literal*", true},
		{"escaped question", "q\\?mark", "q?mark", true},
		{"escaped close bracket", "[\\]]", "]", true},
		{"trailing slash literal", "slash\\", "slash\\", true},
		{"unclosed class", "a[bc", "ab", true},
		{"empty class", "[]", "]", false},
		{"empty negated class", "[^]", "]", true},
		{"empty", "", "", true},
		{"binary", "*", "\x00\xff\r\n", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Match(tc.pattern, tc.value); got != tc.want {
				t.Fatalf("Match(%q, %q) = %v, want %v", tc.pattern, tc.value, got, tc.want)
			}
		})
	}
}

func FuzzMatchNeverPanics(f *testing.F) {
	for _, seed := range [][2]string{{"*", "key"}, {"[", "x"}, {"[^]", "x"}, {"\xff*", "\xff\x00"}} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, pattern, value string) { _ = Match(pattern, value) })
}
