package server

import "testing"

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"user:*", "user:42", true},
		{"user:*", "users:42", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeeello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hello", false},
		{"*:*:*", "a:b:c", true},
		{"*:*:*", "a:b", false},
		{"a**b", "axxb", true},
		{"[abc", "a", true},
		{"", "", true},
		{"", "a", false},
	}
	for _, c := range cases {
		if got := globMatch([]byte(c.pattern), []byte(c.s)); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}
