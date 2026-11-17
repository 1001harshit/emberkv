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
		// Replies of Redis 8.0.5, from the compatibility test in bench/compare.py.
		{1e7, "10000000"}, {1e8, "100000000"}, {123e10, "1230000000000"},
		{123456789012345680, "123456789012345680"}, {1e20, "1e+20"}, {1e21, "1e+21"}, {1e22, "1e+22"},
		{1e-6, "0.000001"}, {1.25e-6, "1.25e-6"}, {0.5, "0.5"},
		{0.0001, "0.0001"}, {-2.5e-5, "-0.000025"}, {1.5e-6, "1.5e-6"}, {1e-7, "1e-7"},
	} {
		if got := formatScore(c.f); got != c.want {
			t.Errorf("formatScore(%v) = %q, want %q", c.f, got, c.want)
		}
	}
}
