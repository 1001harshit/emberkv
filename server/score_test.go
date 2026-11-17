package server

import "testing"

func TestFormatScoreLikeRedis(t *testing.T) {
	for _, c := range []struct {
		f    float64
		want string
	}{
		{0, "0"}, {1, "1"}, {-3, "-3"}, {17.5, "17.5"}, {0.1, "0.1"},
		{-1.5 + 0.1, "-1.4"}, // %.17g would give -1.3999999999999999
		{1234567, "1234567"}, {0.001, "0.001"}, {3.141592653589793, "3.141592653589793"},
		// Where fpconv switches to scientific notation is checked against
		// real Redis by the compatibility test in bench/compare.py.
	} {
		if got := formatScore(c.f); got != c.want {
			t.Errorf("formatScore(%v) = %q, want %q", c.f, got, c.want)
		}
	}
}
