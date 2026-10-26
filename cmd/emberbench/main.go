// Command emberbench is a small load generator for emberkv (or any Redis
// protocol server), in the spirit of redis-benchmark.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	clients := flag.Int("c", 50, "concurrent connections")
	requests := flag.Int("n", 200000, "total requests per test")
	pipeline := flag.Int("P", 1, "commands pipelined per round trip")
	size := flag.Int("d", 64, "value size in bytes")
	keyspace := flag.Int("r", 100000, "number of distinct keys")
	tests := flag.String("t", "set,get", "comma-separated tests: set, get, incr, mset, hset, hget, sadd, mixed, queue, wake")
	flag.Parse()

	value := strings.Repeat("x", *size)
	for _, test := range strings.Split(*tests, ",") {
		test = strings.TrimSpace(test)
		if test == "queue" || test == "wake" {
			if err := runQueue(*addr, test, *clients, *requests, *pipeline); err != nil {
				log.Fatalf("%s: %v", test, err)
			}
			continue
		}
		if err := run(*addr, test, *clients, *requests, *pipeline, *keyspace, value); err != nil {
			log.Fatalf("%s: %v", test, err)
		}
	}
}

func command(test string, rnd *rand.Rand, keyspace int, value string) []string {
	key := "key:" + strconv.Itoa(rnd.Intn(keyspace))
	switch test {
	case "set":
		return []string{"SET", key, value}
	case "get":
		return []string{"GET", key}
	case "incr":
		return []string{"INCR", "counter:" + strconv.Itoa(rnd.Intn(keyspace))}
	case "hset": // fields spread over 1000 hashes
		n := rnd.Intn(keyspace)
		return []string{"HSET", "hash:" + strconv.Itoa(n%1000), "field:" + strconv.Itoa(n), value}
	case "hget":
		n := rnd.Intn(keyspace)
		return []string{"HGET", "hash:" + strconv.Itoa(n%1000), "field:" + strconv.Itoa(n)}
	case "sadd":
		n := rnd.Intn(keyspace)
		return []string{"SADD", "set:" + strconv.Itoa(n%1000), "member:" + strconv.Itoa(n)}
	case "mset": // 10 keys per command, like redis-benchmark
		args := []string{"MSET"}
		for i := 0; i < 10; i++ {
			args = append(args, "key:"+strconv.Itoa(rnd.Intn(keyspace)), value)
		}
		return args
	case "mixed": // 80% reads, 20% writes
		if rnd.Intn(5) == 0 {
			return []string{"SET", key, value}
		}
		return []string{"GET", key}
	}
	return nil
}

func run(addr, test string, clients, requests, pipeline, keyspace int, value string) error {
	if command(test, rand.New(rand.NewSource(1)), 1, value) == nil {
		return fmt.Errorf("unknown test %q", test)
	}
	perClient := requests / clients
	latencies := make([][]time.Duration, clients)
	errs := make(chan error, clients)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			r, w := resp.NewReader(conn), resp.NewWriter(conn)
			rnd := rand.New(rand.NewSource(int64(i)))
			for sent := 0; sent < perClient; {
				batch := min(pipeline, perClient-sent)
				t0 := time.Now()
				for j := 0; j < batch; j++ {
					w.WriteCommand(command(test, rnd, keyspace, value)...)
				}
				if err := w.Flush(); err != nil {
					errs <- err
					return
				}
				for j := 0; j < batch; j++ {
					reply, err := r.ReadReply()
					if err != nil {
						errs <- err
						return
					}
					if e, ok := reply.(resp.Error); ok {
						errs <- e
						return
					}
				}
				latencies[i] = append(latencies[i], time.Since(t0))
				sent += batch
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration { return all[int(float64(len(all)-1)*p)] }
	total := perClient * clients
	fmt.Fprintf(os.Stdout, "%-6s %9d ops  %10.0f ops/sec  p50 %-9v p99 %-9v p99.9 %v\n",
		strings.ToUpper(test), total, float64(total)/elapsed.Seconds(),
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(0.999).Round(time.Microsecond))
	return nil
}

// runQueue measures a job queue built on a list. Producers push items with
// RPUSH (pipelined -P deep) and consumers take them with BLPOP, blocking
// while the list is empty. Each item carries the time it was pushed, and
// the latency reported is push-to-delivery.
//
// The queue test splits the clients in half and measures throughput; its
// latency includes any backlog. The wake test has one producer and the
// rest consumers, so nearly every item is handed to a client already
// blocked: its latency is how fast a push wakes a waiting consumer.
func runQueue(addr, test string, clients, requests, pipeline int) error {
	producers := max(clients/2, 1)
	if test == "wake" {
		producers, pipeline = 1, 1
	}
	consumers := max(clients-producers, 1)
	key := "queue:" + strconv.FormatInt(time.Now().UnixNano(), 36)
	perProducer := requests / producers
	total := int64(perProducer * producers)

	var received atomic.Int64
	var end atomic.Int64 // when the last item arrived, in unix ns
	latencies := make([][]time.Duration, consumers)
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			r, w := resp.NewReader(conn), resp.NewWriter(conn)
			for received.Load() < total {
				w.WriteCommand("BLPOP", key, "0.2")
				if err := w.Flush(); err != nil {
					errs <- err
					return
				}
				reply, err := r.ReadReply()
				if err != nil {
					errs <- err
					return
				}
				switch v := reply.(type) {
				case resp.Error:
					errs <- v
					return
				case []any:
					pushed, err := strconv.ParseInt(string(v[1].([]byte)), 10, 64)
					if err != nil {
						errs <- err
						return
					}
					latencies[i] = append(latencies[i], time.Since(time.Unix(0, pushed)))
					if received.Add(1) == total {
						end.Store(time.Now().UnixNano())
					}
				}
			}
		}(i)
	}
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			r, w := resp.NewReader(conn), resp.NewWriter(conn)
			for sent := 0; sent < perProducer; {
				batch := min(pipeline, perProducer-sent)
				for j := 0; j < batch; j++ {
					w.WriteCommand("RPUSH", key, strconv.FormatInt(time.Now().UnixNano(), 10))
				}
				if err := w.Flush(); err != nil {
					errs <- err
					return
				}
				for j := 0; j < batch; j++ {
					reply, err := r.ReadReply()
					if err != nil {
						errs <- err
						return
					}
					if e, ok := reply.(resp.Error); ok {
						errs <- e
						return
					}
				}
				sent += batch
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case err := <-errs:
		return err
	}
	elapsed := time.Unix(0, end.Load()).Sub(start)
	select {
	case err := <-errs:
		return err
	default:
	}
	if received.Load() != total {
		return errors.New("lost items: received " + strconv.FormatInt(received.Load(), 10) + " of " + strconv.FormatInt(total, 10))
	}

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration { return all[int(float64(len(all)-1)*p)] }
	fmt.Fprintf(os.Stdout, "%-6s %9d items %10.0f items/sec  handoff p50 %-9v p99 %-9v p99.9 %v\n",
		strings.ToUpper(test), total, float64(total)/elapsed.Seconds(),
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(0.999).Round(time.Microsecond))
	return nil
}
