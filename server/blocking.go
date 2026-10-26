package server

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/keyspace"
)

// Blocking list commands. A client that finds every key empty registers a
// waiter on each of them and its connection stops reading commands. When a
// command adds elements to one of those keys, the connection that ran it
// pops for the waiters on that key, oldest first, and hands each its reply:
// the order Redis guarantees. Popping in the pusher's goroutine also means
// no element is ever visible to anyone else between the push and the pop.
//
// A blocking pop that finds an element pops it like LPOP does, without
// touching any of this; the lock is only taken by clients about to block.

// waiter is a client blocked in BLPOP, BRPOP, BLMOVE or BRPOPLPUSH.
type waiter struct {
	branch  string
	keys    [][]byte
	timeout time.Duration   // 0: forever
	st      *keyspace.Store // the client's session
	// pop tries to take an element from key through st for this client. It
	// returns false if key is empty, and the reply to send otherwise, along
	// with a key it pushed to (BLMOVE's destination), which may wake others.
	pop func(st *keyspace.Store, key []byte) (reply func(*resp.Writer), pushed []byte, ok bool)
	// onTimeout writes the reply for a wait that ran out.
	onTimeout func(*resp.Writer)
	done      chan delivery // buffered: receives the reply once
}

// delivery is a reply for a blocked client, made by another connection.
// The client waits for pending, its pop's log write, before sending it, so
// the connection that served it never waits for the disk holding the lock.
type delivery struct {
	reply   func(*resp.Writer)
	pending *keyspace.Pending
}

type blockers struct {
	mu    sync.Mutex
	byKey map[string][]*waiter // branch + "\x00" + key, oldest first
	// n counts waiters registered or about to register. Pushes skip the
	// lock while it is zero, so blocking costs nothing until it is used.
	n atomic.Int64
}

func newBlockers() *blockers { return &blockers{byKey: make(map[string][]*waiter)} }

func waitKey(branch string, key []byte) string { return branch + "\x00" + string(key) }

// count returns the number of blocked clients.
func (b *blockers) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := map[*waiter]bool{}
	for _, ws := range b.byKey {
		for _, w := range ws {
			seen[w] = true
		}
	}
	return len(seen)
}

// block serves w at once if one of its keys has an element. Otherwise,
// with wait, it registers w. The last try and the registration happen
// under one lock, so a push that lands in between cannot be missed: its
// wake-up waits for the lock and then finds w. It returns the reply if w
// was served, with any key it pushed to. It runs on w's own connection, so
// its pops go through w's session.
func (b *blockers) block(w *waiter, wait bool) (reply func(*resp.Writer), pushed []byte, blocked bool) {
	for _, k := range w.keys {
		if reply, pushed, ok := w.pop(w.st, k); ok {
			return reply, pushed, false
		}
	}
	if !wait {
		return w.onTimeout, nil, false
	}
	// Announce the waiter before reading the keys again, so that a push
	// which this read misses sees n > 0 and takes the lock.
	b.n.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range w.keys {
		if reply, pushed, ok := w.pop(w.st, k); ok {
			b.n.Add(-1)
			return reply, pushed, false
		}
	}
	for _, k := range w.keys {
		id := waitKey(w.branch, k)
		if !containsWaiter(b.byKey[id], w) { // BLPOP k k registers once
			b.byKey[id] = append(b.byKey[id], w)
		}
	}
	return nil, nil, true
}

func containsWaiter(ws []*waiter, w *waiter) bool {
	for _, x := range ws {
		if x == w {
			return true
		}
	}
	return false
}

// removeLocked unregisters w from all its keys.
func (b *blockers) removeLocked(w *waiter) {
	for _, k := range w.keys {
		id := waitKey(w.branch, k)
		ws := b.byKey[id]
		for i, x := range ws {
			if x == w {
				ws = append(ws[:i:i], ws[i+1:]...)
				break
			}
		}
		if len(ws) == 0 {
			delete(b.byKey, id)
		} else {
			b.byKey[id] = ws
		}
	}
	b.n.Add(-1)
}

// cancel unregisters w after its timeout or disconnect. If a push served it
// in the meantime, that reply is returned instead.
func (b *blockers) cancel(w *waiter) delivery {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range w.keys {
		if containsWaiter(b.byKey[waitKey(w.branch, k)], w) {
			b.removeLocked(w)
			return delivery{reply: w.onTimeout}
		}
	}
	return <-w.done // served: the reply was sent before it was removed
}

// wake serves the clients waiting on keys of branch, oldest first, for as
// long as the keys have elements.
func (b *blockers) wake(branch string, keys [][]byte) {
	if b.n.Load() == 0 || len(keys) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(keys) > 0 {
		k := keys[0]
		keys = keys[1:]
		for {
			ws := b.byKey[waitKey(branch, k)]
			if len(ws) == 0 {
				break
			}
			w := ws[0]
			p := new(keyspace.Pending)
			reply, pushed, ok := w.pop(w.st.Session(p), k)
			if !ok {
				break
			}
			w.done <- delivery{reply, p}
			b.removeLocked(w)
			if pushed != nil {
				keys = append(keys, pushed)
			}
		}
	}
}

// wakeBranch serves waiters on every key of branch, after a command such as
// ROLLBACK that may have refilled any list.
func (b *blockers) wakeBranch(branch string) {
	if b.n.Load() == 0 {
		return
	}
	b.mu.Lock()
	var keys [][]byte
	prefix := branch + "\x00"
	for id := range b.byKey {
		if strings.HasPrefix(id, prefix) {
			keys = append(keys, []byte(id[len(prefix):]))
		}
	}
	b.mu.Unlock()
	b.wake(branch, keys)
}

// ---- wake-ups requested by commands ----

// wake records that the command added elements to key. The waiters are
// served after the command, or after EXEC for a queued one, so a
// transaction that pushes and then pops sees its own element.
func (c *ctx) wake(key []byte) {
	c.conn.toWake = append(c.conn.toWake, key)
}

// wakeAll records that any list on the branch may have gained elements.
func (c *ctx) wakeAll() { c.conn.wakeBranch = true }

func (s *Server) runWakes(cs *connState) {
	if cs.wakeBranch {
		s.blocked.wakeBranch(cs.branch)
	} else if len(cs.toWake) > 0 {
		s.blocked.wake(cs.branch, cs.toWake)
	}
	cs.toWake, cs.wakeBranch = nil, false
}

// ---- the commands ----

var blockingCommands = map[string]command{
	"blpop":      {cmdBPop(true), -3},
	"brpop":      {cmdBPop(false), -3},
	"blmove":     {cmdBLMove, 6},
	"brpoplpush": {cmdBRPopLPush, 4},
}

// parseTimeout reads a blocking command's timeout in seconds, with Redis's
// errors. Zero means wait forever.
func (c *ctx) parseTimeout(arg []byte) (time.Duration, bool) {
	f, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f > math.MaxInt64/float64(time.Second) {
		c.w.WriteError("ERR timeout is not a float or out of range")
		return 0, false
	}
	if f < 0 {
		c.w.WriteError("ERR timeout is negative")
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}

// errReply is a reply that is an error, for a waiter served with one.
func errReply(err error) func(*resp.Writer) {
	var buf bytes.Buffer
	ec := &ctx{w: resp.NewWriter(&buf)}
	ec.fail(err)
	ec.w.Flush()
	return func(w *resp.Writer) { w.WriteRaw(buf.Bytes()) }
}

// BLPOP/BRPOP key [key ...] timeout
func cmdBPop(left bool) func(*ctx) {
	return func(c *ctx) {
		timeout, ok := c.parseTimeout(c.args[len(c.args)-1])
		if !ok {
			return
		}
		c.blockOn(&waiter{
			keys:    c.args[1 : len(c.args)-1],
			timeout: timeout,
			pop: func(st *keyspace.Store, key []byte) (func(*resp.Writer), []byte, bool) {
				vals, _, err := st.Pop(key, left, 1)
				if err != nil {
					return errReply(err), nil, true
				}
				if len(vals) == 0 {
					return nil, nil, false
				}
				key, v := bytes.Clone(key), vals[0]
				return func(w *resp.Writer) {
					w.WriteArray(2)
					w.WriteBulk(key)
					w.WriteBulk(v)
				}, nil, true
			},
			onTimeout: func(w *resp.Writer) { w.WriteNullArray() },
		})
	}
}

// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
func cmdBLMove(c *ctx) {
	from, ok1 := side(c.args[3])
	to, ok2 := side(c.args[4])
	if !ok1 || !ok2 {
		c.fail(errSyntax)
		return
	}
	c.blockingMove(c.args[1], c.args[2], from, to, c.args[5])
}

// BRPOPLPUSH source destination timeout
func cmdBRPopLPush(c *ctx) { c.blockingMove(c.args[1], c.args[2], false, true, c.args[3]) }

func (c *ctx) blockingMove(src, dst []byte, fromLeft, toLeft bool, timeoutArg []byte) {
	timeout, ok := c.parseTimeout(timeoutArg)
	if !ok {
		return
	}
	dst = bytes.Clone(dst)
	c.blockOn(&waiter{
		keys:    [][]byte{src},
		timeout: timeout,
		pop: func(st *keyspace.Store, key []byte) (func(*resp.Writer), []byte, bool) {
			v, moved, err := st.LMove(key, dst, fromLeft, toLeft)
			if err != nil {
				return errReply(err), nil, true
			}
			if !moved {
				return nil, nil, false
			}
			return func(w *resp.Writer) { w.WriteBulk(v) }, dst, true
		},
		onTimeout: func(w *resp.Writer) { w.WriteNull() },
	})
}

// blockOn serves w at once if it can. Otherwise, outside a transaction, the
// connection blocks after this command (see Server.wait); inside EXEC it
// replies as if the timeout had passed, as Redis does.
func (c *ctx) blockOn(w *waiter) {
	w.branch, w.st = c.conn.branch, c.store()
	w.keys = cloneAll(w.keys)
	w.done = make(chan delivery, 1)
	reply, pushed, blocked := c.s.blocked.block(w, !c.conn.inExec)
	if blocked {
		c.conn.blocked = w
		return
	}
	reply(c.w)
	if pushed != nil {
		c.wake(pushed)
	}
}

func cloneAll(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = bytes.Clone(k)
	}
	return out
}

// wait parks a connection whose command blocked until it is served, its
// timeout passes, the client disconnects or the server closes. It reports
// false if the connection should close.
func (s *Server) wait(cs *connState, r *resp.Reader) bool {
	w := cs.blocked
	cs.blocked = nil

	var timeout <-chan time.Time
	if w.timeout > 0 {
		t := time.NewTimer(w.timeout)
		defer t.Stop()
		timeout = t.C
	}
	// Watch for the client going away. If it already sent more commands,
	// they wait in the buffer and there is nothing to watch.
	var gone chan error
	if r.Buffered() == 0 {
		gone = make(chan error, 1)
		go func() { gone <- r.WaitReadable() }()
		defer func() {
			// Interrupt the watcher and wait for it, so the next read
			// does not race with it.
			cs.netConn.SetReadDeadline(time.Now())
			if gone != nil {
				<-gone
			}
			cs.netConn.SetReadDeadline(time.Time{})
		}()
	}

	var d delivery
	for d.reply == nil {
		select {
		case d = <-w.done:
		case <-timeout:
			d = s.blocked.cancel(w)
		case <-s.quit:
			s.blocked.cancel(w)
			return false
		case err := <-gone:
			gone = nil
			if err != nil {
				s.blocked.cancel(w) // an element served in this instant is lost with the client
				return false
			}
			// More commands arrived; they run once this one is answered.
		}
	}
	if d.pending != nil {
		if err := d.pending.Wait(); err != nil {
			s.logger.Printf("closing connection: %v", err)
			return false
		}
	}
	cs.wmu.Lock()
	defer cs.wmu.Unlock()
	d.reply(cs.w)
	return cs.w.Flush() == nil
}
