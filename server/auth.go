package server

import (
	"crypto/sha256"
	"crypto/subtle"
)

// noAuthCommands may run before a client has authenticated.
var noAuthCommands = map[string]bool{"auth": true, "hello": true, "quit": true}

// RequirePassword makes clients send AUTH <password> before any other
// command, like Redis's requirepass.
func (s *Server) RequirePassword(password string) { s.password = password }

// cmdAuth is AUTH <password> or AUTH <username> <password>; the only user is
// "default", as in Redis without ACLs.
func cmdAuth(c *ctx) {
	if c.s.password == "" {
		c.w.WriteError("ERR AUTH <password> called without any password configured for the default user. " +
			"Are you sure your configuration is correct?")
		return
	}
	user, pass := "default", c.args[1]
	switch len(c.args) {
	case 2:
	case 3:
		user, pass = string(c.args[1]), c.args[2]
	default:
		c.fail(errSyntax)
		return
	}
	// Compare digests in constant time, so timing reveals nothing about the
	// password, not even its length.
	want, got := sha256.Sum256([]byte(c.s.password)), sha256.Sum256(pass)
	if user != "default" || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		c.conn.authed = false
		c.w.WriteError("WRONGPASS invalid username-password pair or user is disabled.")
		return
	}
	c.conn.authed = true
	c.ok()
}
