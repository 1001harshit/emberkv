package server

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/lsm"
)

// next reads one pushed message or reply.
func (c *client) next() any {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})
	v, err := c.r.ReadReply()
	if err != nil {
		c.t.Fatal(err)
	}
	return normalize(v)
}

func (c *client) send(args ...string) {
	c.w.WriteCommand(args...)
	c.w.Flush()
}

func TestPubSub(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	sub, psub, pub := dial(t, addr), dial(t, addr), dial(t, addr)

	sub.send("SUBSCRIBE", "news", "sport")
	if got := sub.next(); !reflect.DeepEqual(got, list("subscribe", "news", int64(1))) {
		t.Fatalf("subscribe reply = %#v", got)
	}
	if got := sub.next(); !reflect.DeepEqual(got, list("subscribe", "sport", int64(2))) {
		t.Fatalf("subscribe reply = %#v", got)
	}
	psub.send("PSUBSCRIBE", "news.*")
	psub.next()

	pub.expect(int64(1), "PUBLISH", "news", "hello")
	pub.expect(int64(1), "PUBLISH", "news.tech", "go 1.30")
	pub.expect(int64(0), "PUBLISH", "nobody", "x")
	if got := sub.next(); !reflect.DeepEqual(got, list("message", "news", "hello")) {
		t.Fatalf("message = %#v", got)
	}
	if got := psub.next(); !reflect.DeepEqual(got, list("pmessage", "news.*", "news.tech", "go 1.30")) {
		t.Fatalf("pmessage = %#v", got)
	}

	// Subscriber mode only allows (un)subscribing, PING and QUIT.
	sub.send("GET", "k")
	if e, ok := sub.next().(resp.Error); !ok || !strings.HasPrefix(string(e), "ERR Can't execute 'get'") {
		t.Fatalf("GET in subscriber mode = %#v", e)
	}
	sub.send("PING")
	if got := sub.next(); !reflect.DeepEqual(got, list("pong", "")) {
		t.Fatalf("PING in subscriber mode = %#v", got)
	}

	pub.expect(list("news", "sport"), "PUBSUB", "CHANNELS")
	pub.expect(list("news", int64(1), "none", int64(0)), "PUBSUB", "NUMSUB", "news", "none")
	pub.expect(int64(1), "PUBSUB", "NUMPAT")

	// Unsubscribing from everything returns to normal mode.
	sub.send("UNSUBSCRIBE")
	if got := sub.next(); !reflect.DeepEqual(got, list("unsubscribe", "news", int64(1))) {
		t.Fatalf("unsubscribe = %#v", got)
	}
	sub.next()
	sub.expect(nil, "GET", "k")
	pub.expect(int64(0), "PUBLISH", "news", "gone")
	pub.expect(list(), "PUBSUB", "CHANNELS")
}

// TestSlowSubscriberIsDisconnected checks that a subscriber that never reads
// cannot make PUBLISH block.
func TestSlowSubscriberIsDisconnected(t *testing.T) {
	_, addr := startServer(t, lsm.Options{})
	slow, pub := dial(t, addr), dial(t, addr)
	slow.send("SUBSCRIBE", "firehose")
	slow.next()
	payload := strings.Repeat("x", 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 3*outboxSize; i++ {
			pub.do("PUBLISH", "firehose", payload)
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("PUBLISH blocked on a slow subscriber")
	}
	pub.expect(int64(0), "PUBSUB", "NUMPAT")
	deadline := time.Now().Add(5 * time.Second)
	for {
		v := pub.do("PUBSUB", "NUMSUB", "firehose").([]any)
		if v[1] == int64(0) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the slow subscriber was never disconnected")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
