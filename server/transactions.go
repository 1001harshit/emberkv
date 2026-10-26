package server

import (
	"strings"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/keyspace"
)

// txControl commands run immediately even inside MULTI; everything else is
// queued until EXEC.
var txControl = map[string]bool{"exec": true, "discard": true, "multi": true, "watch": true, "quit": true}

// watch remembers a key's version when WATCH was called.
type watch struct {
	st      *keyspace.Store
	key     []byte
	version uint64
}

// failTx marks an open transaction as failed, so EXEC aborts it, as Redis
// does when queuing a command fails.
func (cs *connState) failTx() {
	if cs.inMulti {
		cs.txFailed = true
	}
}

func (cs *connState) resetTx() {
	cs.inMulti, cs.txFailed, cs.queue, cs.watches = false, false, nil, nil
}

func cmdMulti(c *ctx) {
	if c.conn.inMulti {
		c.w.WriteError("ERR MULTI calls can not be nested")
		return
	}
	c.conn.inMulti = true
	c.ok()
}

func cmdDiscard(c *ctx) {
	if !c.conn.inMulti {
		c.w.WriteError("ERR DISCARD without MULTI")
		return
	}
	c.conn.resetTx()
	c.ok()
}

// cmdWatch records each key's version; EXEC refuses to run if any changed.
func cmdWatch(c *ctx) {
	if c.conn.inMulti {
		c.w.WriteError("ERR WATCH inside MULTI is not allowed")
		return
	}
	for _, k := range c.args[1:] {
		v, err := c.store().WatchVersion(k)
		if err != nil {
			c.fail(err)
			return
		}
		c.conn.watches = append(c.conn.watches, watch{st: c.store(), key: k, version: v})
	}
	c.ok()
}

func cmdUnwatch(c *ctx) {
	c.conn.watches = nil
	c.ok()
}

// exec runs the queued commands with txMu held exclusively, so no other
// client's command interleaves. Like Redis, a command that fails at run time
// returns its error inside the reply array and the others still apply.
func (s *Server) exec(cs *connState, w *resp.Writer) (quit bool) {
	if !cs.inMulti {
		w.WriteError("ERR EXEC without MULTI")
		return false
	}
	queue, failed, watches := cs.queue, cs.txFailed, cs.watches
	cs.resetTx()
	if failed {
		w.WriteError("EXECABORT Transaction discarded because of previous errors.")
		return false
	}

	s.txMu.Lock()
	defer s.txMu.Unlock()
	for _, wt := range watches {
		v, err := wt.st.WatchVersion(wt.key)
		if err != nil || v != wt.version {
			w.WriteNullArray() // a watched key changed: the transaction is cancelled
			return false
		}
	}
	w.WriteArray(len(queue))
	cs.inExec = true
	for _, args := range queue {
		if s.run(cs, w, commands[strings.ToLower(string(args[0]))], args) {
			quit = true
		}
	}
	cs.inExec = false
	s.runWakes(cs) // still holding txMu: waiters are served after the transaction, not inside it
	return quit
}
