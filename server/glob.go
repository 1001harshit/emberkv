package server

// globMatch reports whether s matches a Redis-style glob pattern: '*' matches
// any sequence of bytes, '?' any single byte, '[abc]' one byte from a set
// ('[^abc]' negates it and '[a-z]' is a range), and '\x' the literal byte x.
func globMatch(pattern, s []byte) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if globMatch(pattern[1:], s[i:]) {
					return true
				}
			}
			return false

		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]

		case '[':
			if len(s) == 0 {
				return false
			}
			p := pattern[1:]
			negate := len(p) > 0 && p[0] == '^'
			if negate {
				p = p[1:]
			}
			matched := false
			for len(p) > 0 && p[0] != ']' {
				switch {
				case p[0] == '\\' && len(p) >= 2:
					matched = matched || p[1] == s[0]
					p = p[2:]
				case len(p) >= 3 && p[1] == '-' && p[2] != ']':
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					matched = matched || (s[0] >= lo && s[0] <= hi)
					p = p[3:]
				default:
					matched = matched || p[0] == s[0]
					p = p[1:]
				}
			}
			if matched == negate {
				return false
			}
			s = s[1:]
			if len(p) == 0 { // unterminated class runs to the end of the pattern
				return len(s) == 0
			}
			pattern = p // now at ']', skipped below

		case '\\':
			if len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			fallthrough

		default:
			if len(s) == 0 || pattern[0] != s[0] {
				return false
			}
			s = s[1:]
		}
		pattern = pattern[1:]
	}
	return len(s) == 0
}
