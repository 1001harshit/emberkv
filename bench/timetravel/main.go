// Command timetravel compares emberkv with other databases that can read
// the past: etcd, immudb and Dolt. Every target runs the same workload and
// every answer is checked:
//
//  1. write K keys in R rounds (round r stores "r:<key>"), with a checkpoint
//     after round R/2;
//  2. read every key's latest value (must be round R);
//  3. read every key as of the checkpoint (must be round R/2);
//  4. fetch each key's history of versions;
//  5. create a branch, where the database has branches.
//
// Each database is used through its own client and its own way of reading
// the past; see the target implementations for exactly how.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// target is one database under test. Methods must be safe for concurrent use
// by different workers; worker is in [0, workers).
type target interface {
	Name() string
	Setup(ctx context.Context, workers int) error
	Set(ctx context.Context, worker int, key, value string) error
	Get(ctx context.Context, worker int, key string) (string, error)
	// Checkpoint marks "now"; GetAtCheckpoint reads as of that mark.
	Checkpoint(ctx context.Context, round int) error
	GetAtCheckpoint(ctx context.Context, worker int, key string) (string, error)
	// History returns how many versions of key the database returned.
	History(ctx context.Context, worker int, key string) (int, error)
	// Branch creates a branch and reports whether the database supports it.
	Branch(ctx context.Context) (bool, error)
	// Notes describe how the target implements each operation.
	Notes() map[string]string
	Close()
}

type phase struct {
	Ops      int     `json:"ops"`
	Errors   int64   `json:"errors"`
	Wrong    int64   `json:"wrong"`
	Seconds  float64 `json:"seconds"`
	OpsPerS  float64 `json:"ops_per_s"`
	P50Ms    float64 `json:"p50_ms"`
	P99Ms    float64 `json:"p99_ms"`
	FirstErr string  `json:"first_error,omitempty"`
}

type result struct {
	Target      string            `json:"target"`
	Write       phase             `json:"write"`
	ReadLatest  phase             `json:"read_latest"`
	ReadPast    phase             `json:"read_past"`
	History     phase             `json:"history"`
	Versions    float64           `json:"history_versions_avg"`
	BranchMs    float64           `json:"branch_ms"` // -1: unsupported
	BranchError string            `json:"branch_error,omitempty"`
	DiskBytes   int64             `json:"disk_bytes"`
	Notes       map[string]string `json:"notes"`
	Error       string            `json:"error,omitempty"`
}

// run executes fn for each of n items across workers, timing each call.
// fn returns whether the answer was correct.
func run(ctx context.Context, workers, n int, fn func(worker, i int) (bool, error)) phase {
	var next atomic.Int64
	var errs, wrong atomic.Int64
	var firstErr atomic.Value
	lat := make([][]time.Duration, workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n || ctx.Err() != nil {
					return
				}
				t0 := time.Now()
				ok, err := fn(w, i)
				lat[w] = append(lat[w], time.Since(t0))
				switch {
				case err != nil:
					if errs.Add(1) == 1 {
						firstErr.Store(err.Error())
					}
				case !ok:
					wrong.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := phase{Ops: n, Errors: errs.Load(), Wrong: wrong.Load(), Seconds: secs, OpsPerS: float64(n) / secs}
	if len(all) > 0 {
		p.P50Ms = float64(all[len(all)/2]) / 1e6
		p.P99Ms = float64(all[len(all)*99/100]) / 1e6
	}
	if v := firstErr.Load(); v != nil {
		p.FirstErr = v.(string)
	}
	return p
}

func key(i int) string { return fmt.Sprintf("key:%05d", i) }

func bench(ctx context.Context, t target, keys, rounds, workers, historyKeys int, disk string) (res result) {
	res = result{Target: t.Name(), BranchMs: -1, Notes: t.Notes()}
	defer func() {
		if r := recover(); r != nil {
			res.Error = fmt.Sprint(r)
		}
	}()
	// Servers in containers can accept connections before they are ready.
	var err error
	for deadline := time.Now().Add(90 * time.Second); ; {
		if err = t.Setup(ctx, workers); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		res.Error = "setup: " + err.Error()
		return res
	}
	defer t.Close()
	checkpointRound := rounds / 2

	// 1. Writes, round by round so each round's value is known.
	var w phase
	for r := 1; r <= rounds; r++ {
		p := run(ctx, workers, keys, func(wk, i int) (bool, error) {
			return true, t.Set(ctx, wk, key(i), fmt.Sprintf("%d:%s", r, key(i)))
		})
		w.Ops += p.Ops
		w.Errors += p.Errors
		w.Seconds += p.Seconds
		if w.FirstErr == "" {
			w.FirstErr = p.FirstErr
		}
		// Keep the slowest round's percentiles as a conservative figure.
		w.P50Ms, w.P99Ms = max(w.P50Ms, p.P50Ms), max(w.P99Ms, p.P99Ms)
		if r == checkpointRound {
			if err := t.Checkpoint(ctx, r); err != nil {
				res.Error = "checkpoint: " + err.Error()
				return res
			}
		}
	}
	w.OpsPerS = float64(w.Ops) / w.Seconds
	res.Write = w

	order := rand.New(rand.NewSource(1)).Perm(keys)
	res.ReadLatest = run(ctx, workers, keys, func(wk, i int) (bool, error) {
		k := key(order[i])
		v, err := t.Get(ctx, wk, k)
		return v == fmt.Sprintf("%d:%s", rounds, k), err
	})
	res.ReadPast = run(ctx, workers, keys, func(wk, i int) (bool, error) {
		k := key(order[i])
		v, err := t.GetAtCheckpoint(ctx, wk, k)
		return v == fmt.Sprintf("%d:%s", checkpointRound, k), err
	})
	var versions atomic.Int64
	res.History = run(ctx, workers, historyKeys, func(wk, i int) (bool, error) {
		n, err := t.History(ctx, wk, key(order[i]))
		versions.Add(int64(n))
		return n > 0, err
	})
	res.Versions = float64(versions.Load()) / float64(historyKeys)

	t0 := time.Now()
	ok, err := t.Branch(ctx)
	if ok {
		res.BranchMs = float64(time.Since(t0).Microseconds()) / 1000
	}
	if err != nil {
		res.BranchError = err.Error()
	}
	if disk != "" {
		res.DiskBytes = du(disk)
	}
	return res
}

// du returns the size of a directory, using sudo for root-owned Docker data.
func du(path string) int64 {
	out, err := exec.Command("sudo", "du", "-sb", path).Output()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.Fields(string(out))[0], 10, 64)
	return n
}

func main() {
	keys := flag.Int("keys", 10000, "distinct keys")
	rounds := flag.Int("rounds", 10, "times each key is written")
	// immudb rejects more than about 30 concurrent requests by default, so
	// every target gets 24 clients.
	workers := flag.Int("workers", 24, "concurrent clients")
	historyKeys := flag.Int("history-keys", 1000, "keys whose history is fetched")
	targets := flag.String("targets", "emberkv,etcd,immudb,dolt", "comma-separated targets")
	disks := flag.String("disk", "", "comma-separated target=path of each data directory")
	out := flag.String("out", "timetravel.json", "results file")
	flag.Parse()

	diskOf := map[string]string{}
	for _, kv := range strings.Split(*disks, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			diskOf[k] = v
		}
	}
	all := map[string]target{
		"emberkv": &emberTarget{addr: "127.0.0.1:6380"},
		"etcd":    &etcdTarget{endpoint: "127.0.0.1:2379"},
		"immudb":  &immuTarget{host: "127.0.0.1", port: 3322},
		"dolt":    &doltTarget{dsn: "root:bench@tcp(127.0.0.1:3306)/"},
	}
	ctx := context.Background()
	var results []result
	for _, name := range strings.Split(*targets, ",") {
		t, ok := all[name]
		if !ok {
			fmt.Fprintln(os.Stderr, "unknown target", name)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "== %s\n", name)
		r := bench(ctx, t, *keys, *rounds, *workers, *historyKeys, diskOf[name])
		b, _ := json.Marshal(r)
		fmt.Fprintln(os.Stderr, string(b))
		results = append(results, r)
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	md := report(results, *keys, *rounds, *workers)
	fmt.Print(md)
	if f := os.Getenv("GITHUB_STEP_SUMMARY"); f != "" {
		os.WriteFile(f, []byte(md), 0o644)
	}
}

func report(rs []result, keys, rounds, workers int) string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	w("# Time-travel databases compared\n")
	w("%d keys written %d times each (%d writes) by %d concurrent clients; checkpoint after round %d. "+
		"Every answer is checked.\n", keys, rounds, keys*rounds, workers, rounds/2)
	w("| | %s |", joinNames(rs))
	w("|---|%s", strings.Repeat("---|", len(rs)))
	row := func(label string, f func(r result) string) {
		cells := make([]string, len(rs))
		for i, r := range rs {
			if r.Error != "" {
				cells[i] = "error"
			} else {
				cells[i] = f(r)
			}
		}
		w("| %s | %s |", label, strings.Join(cells, " | "))
	}
	tput := func(p phase) string {
		s := fmt.Sprintf("%s/s (p50 %.2f ms)", human(p.OpsPerS), p.P50Ms)
		if p.Errors > 0 || p.Wrong > 0 {
			s += fmt.Sprintf(" **%d errors, %d wrong**", p.Errors, p.Wrong)
		}
		return s
	}
	row("Writes", func(r result) string { return tput(r.Write) })
	row("Read latest", func(r result) string { return tput(r.ReadLatest) })
	row("Read as of checkpoint", func(r result) string { return tput(r.ReadPast) })
	row("Full history of a key", func(r result) string {
		return fmt.Sprintf("%s (%.0f versions)", tput(r.History), r.Versions)
	})
	row("Create a branch", func(r result) string {
		switch {
		case r.BranchError != "":
			return "error: " + r.BranchError
		case r.BranchMs < 0:
			return "not supported"
		}
		return fmt.Sprintf("%.0f ms", r.BranchMs)
	})
	row("Disk after the run", func(r result) string { return fmt.Sprintf("%.1f MiB", float64(r.DiskBytes)/1048576) })
	w("")
	for _, r := range rs {
		if r.Error != "" {
			w("- **%s failed:** %s", r.Target, r.Error)
		}
		for _, k := range []string{"read_past", "history", "branch"} {
			w("- %s %s: %s", r.Target, k, r.Notes[k])
		}
	}
	return b.String()
}

func joinNames(rs []result) string {
	names := make([]string, len(rs))
	for i, r := range rs {
		names[i] = r.Target
	}
	return strings.Join(names, " | ")
}

func human(n float64) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1fk", n/1000)
	}
	return fmt.Sprintf("%.0f", n)
}
