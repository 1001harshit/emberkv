// Command emberbench is a small load generator for emberkv (or any Redis
// protocol server), in the spirit of redis-benchmark.
package main

import (
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
	tests := flag.String("t", "set,get", "comma-separated tests: set, get, incr, mixed")
	flag.Parse()

	value := strings.Repeat("x", *size)
	for _, test := range strings.Split(*tests, ",") {
		test = strings.TrimSpace(test)
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
