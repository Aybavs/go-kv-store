// Package glob matches Redis-style glob patterns against binary strings.
package glob

// Match reports whether pattern matches value. Both strings are matched byte by
// byte, including bytes that are not valid UTF-8.
func Match(pattern, value string) bool {
	pi, vi := 0, 0
	starPattern, starValue := -1, -1

	for vi < len(value) {
		if pi < len(pattern) && pattern[pi] == '*' {
			for pi < len(pattern) && pattern[pi] == '*' {
				pi++
			}
			if pi == len(pattern) {
				return true
			}
			starPattern, starValue = pi, vi
			continue
		}

		next, matched := matchOne(pattern, pi, value[vi])
		if matched {
			pi, vi = next, vi+1
			continue
		}
		if starPattern >= 0 && starValue < len(value) {
			starValue++
			pi, vi = starPattern, starValue
			continue
		}
		return false
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

func matchOne(pattern string, at int, value byte) (next int, matched bool) {
	if at >= len(pattern) {
		return at, false
	}

	switch pattern[at] {
	case '?':
		return at + 1, true
	case '\\':
		if at+1 < len(pattern) {
			return at + 2, pattern[at+1] == value
		}
		return at + 1, value == '\\'
	case '[':
		return matchClass(pattern, at, value)
	default:
		return at + 1, pattern[at] == value
	}
}

func matchClass(pattern string, at int, value byte) (next int, matched bool) {
	i := at + 1
	negated := i < len(pattern) && pattern[i] == '^'
	if negated {
		i++
	}

	for i < len(pattern) {
		switch {
		case pattern[i] == ']':
			if negated {
				matched = !matched
			}
			return i + 1, matched
		case pattern[i] == '\\' && i+1 < len(pattern):
			i++
			matched = matched || pattern[i] == value
			i++
		case i+2 < len(pattern) && pattern[i+1] == '-':
			lo, hi := pattern[i], pattern[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			matched = matched || (value >= lo && value <= hi)
			i += 3
		default:
			matched = matched || pattern[i] == value
			i++
		}
	}

	if negated {
		matched = !matched
	}
	return len(pattern), matched
}
