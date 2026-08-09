// Package cmdgen produces seeded, bounded command sequences over a small key
// space, for differential testing against an independent oracle. Handwritten
// scenarios cover the corners their author thought of; a sequence like EXPIRE,
// INCR, TTL, PERSIST, INCR reaches one nobody would write down.
//
// Two properties make it usable rather than merely random.
//
// A sequence is a pure function of its seed — no clock, no global rand, no map
// iteration — so a failure is reproducible from the seed alone.
//
// Every generated TTL is long. Both consumers compare two runs against each
// other, so a short expiry would make them time-dependent. Expiry transitions
// belong in the handwritten scenarios, where the wait is deliberate.
//
// It generates text and does not know what runs it. A test pins that it never
// reaches the server binary.
package cmdgen

import "math/rand"

// Step is one command. Step[0] is the name.
type Step []string

// Config bounds a sequence. The key space is small on purpose: collisions are
// where the interesting states are, and four keys collide constantly.
type Config struct {
	Keys  []string
	Steps int
}

func DefaultConfig() Config {
	return Config{
		Keys:  []string{"k0", "k1", "k2", "k3"},
		Steps: 120,
	}
}

// values are chosen so the transitions worth reaching are reachable: numbers to
// increment, both int64 boundaries so overflow is hit rather than approached, a
// non-numeric string and an empty one so INCR is refused, and a binary value so
// framing is exercised by every sequence.
//
// "+5" and "07" are here for a measured reason. Redis rejects both and Go's
// strconv accepts both, so they are the exact values an implementation written
// on the standard library gets wrong — and no sequence could ever produce them
// on its own, since INCR stores its result in canonical decimal. Without them
// the generator cannot reach the divergence it exists to find: verified by
// mutation, where a parser that accepted leading zeros passed every seed until
// these two values were added.
var values = []string{
	"0",
	"5",
	"-3",
	"+5",
	"07",
	"9223372036854775807",
	"-9223372036854775808",
	"abc",
	"",
	"x\x00y\r\nz",
}

// Only long expiries, per the package comment. PX is here as well as EX so the
// millisecond path is generated too.
var expiries = [][2]string{
	{"EX", "100"},
	{"EX", "1000"},
	{"PX", "100000"},
}

// verbs is the generated vocabulary. Weights are uniform: skewing them would be
// a claim about where bugs are, and the point of generating sequences is not
// having to make that claim.
var verbs = []string{
	"SET", "SETEX", "GET", "MGET", "DEL", "EXISTS",
	"INCR", "DECR", "EXPIRE", "TTL", "PERSIST",
}

// Verbs reports the command names a default sequence can emit. Tests use it to
// check the generator reaches all of them; a generator that only ever emitted
// PING would satisfy every other assertion about it.
func Verbs() []string {
	out := make([]string, len(verbs))
	copy(out, verbs)
	return out
}

// Sequence returns cfg.Steps commands drawn from seed. It is deterministic: the
// same seed and config always produce the same steps.
func Sequence(seed int64, cfg Config) []Step {
	if len(cfg.Keys) == 0 || cfg.Steps <= 0 {
		return nil
	}
	//nolint:gosec // Reproducibility is the requirement here, not unpredictability.
	rng := rand.New(rand.NewSource(seed))

	key := func() string { return cfg.Keys[rng.Intn(len(cfg.Keys))] }
	// One to three distinct positions, so duplicates and misses both occur.
	keys := func() []string {
		n := 1 + rng.Intn(3)
		out := make([]string, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, key())
		}
		return out
	}

	out := make([]Step, 0, cfg.Steps)
	for len(out) < cfg.Steps {
		switch verbs[rng.Intn(len(verbs))] {
		case "SET":
			out = append(out, Step{"SET", key(), values[rng.Intn(len(values))]})
		case "SETEX":
			ex := expiries[rng.Intn(len(expiries))]
			out = append(out, Step{"SET", key(), values[rng.Intn(len(values))], ex[0], ex[1]})
		case "GET":
			out = append(out, Step{"GET", key()})
		case "MGET":
			out = append(out, append(Step{"MGET"}, keys()...))
		case "DEL":
			out = append(out, append(Step{"DEL"}, keys()...))
		case "EXISTS":
			out = append(out, append(Step{"EXISTS"}, keys()...))
		case "INCR":
			out = append(out, Step{"INCR", key()})
		case "DECR":
			out = append(out, Step{"DECR", key()})
		case "EXPIRE":
			// Seconds only, and long: see the package comment.
			out = append(out, Step{"EXPIRE", key(), []string{"100", "1000"}[rng.Intn(2)]})
		case "TTL":
			out = append(out, Step{"TTL", key()})
		case "PERSIST":
			out = append(out, Step{"PERSIST", key()})
		}
	}
	return out
}

// String renders a step for a failure message.
func (s Step) String() string {
	out := ""
	for i, part := range s {
		if i > 0 {
			out += " "
		}
		out += quote(part)
	}
	return out
}

// quote keeps binary values readable in output without pulling in strconv's
// escaping rules, which would render a CRLF as text that looks like two steps.
func quote(s string) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 0, len(s)+2)
	out = append(out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			out = append(out, '\\', c)
		case c == '\r':
			out = append(out, '\\', 'r')
		case c == '\n':
			out = append(out, '\\', 'n')
		case c < 0x20 || c > 0x7e:
			out = append(out, '\\', 'x', hex[c>>4], hex[c&0xf])
		default:
			out = append(out, c)
		}
	}
	return string(append(out, '"'))
}
