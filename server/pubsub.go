package server

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// subscribedOK commands are the only ones a connection may run once it has
// subscribed, as in Redis's RESP2 subscriber mode.
var subscribedOK = map[string]bool{
	"subscribe": true, "psubscribe": true, "unsubscribe": true, "punsubscribe": true,
	"ping": true, "quit": true,
}

// outboxSize bounds the messages queued for one subscriber. A subscriber
// that falls this far behind is disconnected, like Redis's pubsub
// client-output-buffer-limit, so it cannot make publishers wait.
const outboxSize = 4096

// subscriber is a connection in subscriber mode.
type subscriber struct {
	cs       *connState
	channels map[string]bool
	patterns map[string]bool
	outbox   chan []byte
	done     chan struct{}
	once     sync.Once
}

func (sb *subscriber) count() int64 { return int64(len(sb.channels) + len(sb.patterns)) }

// deliver sends queued messages to the client, sharing its writer with the
// command loop through wmu.
func (sb *subscriber) deliver() {
	for {
		select {
		case <-sb.done:
			return
		case msg := <-sb.outbox:
			sb.cs.wmu.Lock()
			sb.cs.w.WriteRaw(msg)
			// Write everything else already queued before flushing.
			for more := true; more; {
				select {
				case m := <-sb.outbox:
					sb.cs.w.WriteRaw(m)
				default:
					more = false
				}
			}
			err := sb.cs.w.Flush()
			sb.cs.wmu.Unlock()
			if err != nil {
				sb.cs.netConn.Close()
				return
			}
		}
	}
}

// hub routes published messages to subscribers.
type hub struct {
	mu       sync.RWMutex
	channels map[string]map[*subscriber]bool
	patterns map[string]map[*subscriber]bool
}

func newHub() *hub {
	return &hub{channels: map[string]map[*subscriber]bool{}, patterns: map[string]map[*subscriber]bool{}}
}

func (h *hub) add(set map[string]map[*subscriber]bool, name string, sb *subscriber) {
	if set[name] == nil {
		set[name] = map[*subscriber]bool{}
	}
	set[name][sb] = true
}

func (h *hub) remove(set map[string]map[*subscriber]bool, name string, sb *subscriber) {
	delete(set[name], sb)
	if len(set[name]) == 0 {
		delete(set, name)
	}
}

// drop removes every subscription of a closing connection.
func (h *hub) drop(cs *connState) {
	sb := cs.sub
	if sb == nil {
		return
	}
	h.mu.Lock()
	for ch := range sb.channels {
		h.remove(h.channels, ch, sb)
	}
	for p := range sb.patterns {
		h.remove(h.patterns, p, sb)
	}
	h.mu.Unlock()
	sb.once.Do(func() { close(sb.done) })
}

func encodeArray(parts ...[]byte) []byte {
	b := []byte("*" + strconv.Itoa(len(parts)) + "\r\n")
	for _, p := range parts {
		b = append(b, "$"+strconv.Itoa(len(p))+"\r\n"...)
		b = append(append(b, p...), "\r\n"...)
	}
	return b
}

// publish delivers payload to channel's subscribers and to every pattern
// that matches it, returning how many received it.
func (h *hub) publish(channel, payload []byte) int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var n int64
	send := func(sb *subscriber, msg []byte) {
		select {
		case sb.outbox <- msg:
			n++
		default:
			// Too far behind: disconnect it rather than block the publisher.
			go sb.cs.netConn.Close()
		}
	}
	if subs := h.channels[string(channel)]; len(subs) > 0 {
		msg := encodeArray([]byte("message"), channel, payload)
		for sb := range subs {
			send(sb, msg)
		}
	}
	for p, subs := range h.patterns {
		if globMatch([]byte(p), channel) {
			msg := encodeArray([]byte("pmessage"), []byte(p), channel, payload)
			for sb := range subs {
				send(sb, msg)
			}
		}
	}
	return n
}

func (c *ctx) subscriber() *subscriber {
	if c.conn.sub == nil {
		sb := &subscriber{cs: c.conn, channels: map[string]bool{}, patterns: map[string]bool{},
			outbox: make(chan []byte, outboxSize), done: make(chan struct{})}
		c.conn.sub = sb
		go sb.deliver()
	}
	return c.conn.sub
}

func (c *ctx) writeSubReply(kind string, name []byte, count int64) {
	c.w.WriteArray(3)
	c.w.WriteBulkString(kind)
	if name == nil {
		c.w.WriteNull()
	} else {
		c.w.WriteBulk(name)
	}
	c.w.WriteInt(count)
}

func cmdSubscribe(pattern bool) func(*ctx) {
	return func(c *ctx) {
		h := c.s.pubsub
		sb := c.subscriber()
		kind, mine, set := "subscribe", sb.channels, h.channels
		if pattern {
			kind, mine, set = "psubscribe", sb.patterns, h.patterns
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, name := range c.args[1:] {
			mine[string(name)] = true
			h.add(set, string(name), sb)
			c.writeSubReply(kind, name, sb.count())
		}
	}
}

// cmdUnsubscribe leaves the named channels (or patterns), or all of them.
// The connection leaves subscriber mode when nothing is left.
func cmdUnsubscribe(pattern bool) func(*ctx) {
	return func(c *ctx) {
		h := c.s.pubsub
		kind := "unsubscribe"
		if pattern {
			kind = "punsubscribe"
		}
		sb := c.conn.sub
		if sb == nil {
			c.writeSubReply(kind, nil, 0)
			return
		}
		mine, set := sb.channels, h.channels
		if pattern {
			mine, set = sb.patterns, h.patterns
		}
		names := c.args[1:]
		if len(names) == 0 {
			for n := range mine {
				names = append(names, []byte(n))
			}
			sort.Slice(names, func(i, j int) bool { return string(names[i]) < string(names[j]) })
		}
		h.mu.Lock()
		for _, name := range names {
			delete(mine, string(name))
			h.remove(set, string(name), sb)
			c.writeSubReply(kind, name, sb.count())
		}
		h.mu.Unlock()
		if len(names) == 0 {
			c.writeSubReply(kind, nil, sb.count())
		}
		if sb.count() == 0 {
			h.drop(c.conn)
			c.conn.sub = nil
		}
	}
}

func cmdPublish(c *ctx) {
	c.w.WriteInt(c.s.pubsub.publish(c.args[1], c.args[2]))
}

// cmdPubSub is PUBSUB CHANNELS [pattern] | NUMSUB [channel ...] | NUMPAT.
func cmdPubSub(c *ctx) {
	h := c.s.pubsub
	h.mu.RLock()
	defer h.mu.RUnlock()
	switch strings.ToUpper(string(c.args[1])) {
	case "CHANNELS":
		var names []string
		for ch := range h.channels {
			if len(c.args) < 3 || globMatch(c.args[2], []byte(ch)) {
				names = append(names, ch)
			}
		}
		sort.Strings(names)
		c.w.WriteArray(len(names))
		for _, n := range names {
			c.w.WriteBulkString(n)
		}
	case "NUMSUB":
		c.w.WriteArray(2 * (len(c.args) - 2))
		for _, ch := range c.args[2:] {
			c.w.WriteBulk(ch)
			c.w.WriteInt(int64(len(h.channels[string(ch)])))
		}
	case "NUMPAT":
		c.w.WriteInt(int64(len(h.patterns)))
	default:
		c.fail(errSyntax)
	}
}
