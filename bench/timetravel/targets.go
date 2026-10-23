package main

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ---- emberkv: Redis protocol, ASOF <unix ms>, HISTORY, BRANCH CREATE ----

type emberTarget struct {
	addr string
	rdb  *redis.Client
	cpMs int64
}

func (t *emberTarget) Name() string { return "emberkv" }
func (t *emberTarget) Notes() map[string]string {
	return map[string]string{
		"read_past": "ASOF <unix ms> GET key",
		"history":   "HISTORY key COUNT 100 (every write is a version)",
		"branch":    "BRANCH CREATE",
	}
}
func (t *emberTarget) Setup(ctx context.Context, workers int) error {
	t.rdb = redis.NewClient(&redis.Options{Addr: t.addr, PoolSize: workers})
	return t.rdb.Ping(ctx).Err()
}
func (t *emberTarget) Set(ctx context.Context, _ int, k, v string) error {
	return t.rdb.Set(ctx, k, v, 0).Err()
}
func (t *emberTarget) Get(ctx context.Context, _ int, k string) (string, error) {
	return t.rdb.Get(ctx, k).Result()
}
func (t *emberTarget) Checkpoint(ctx context.Context, _ int) error {
	// Writes are acknowledged only when durable, so every write of the round
	// happened before now; leave a millisecond on each side of the mark.
	time.Sleep(2 * time.Millisecond)
	t.cpMs = time.Now().UnixMilli()
	time.Sleep(2 * time.Millisecond)
	return nil
}
func (t *emberTarget) GetAtCheckpoint(ctx context.Context, _ int, k string) (string, error) {
	return t.rdb.Do(ctx, "ASOF", t.cpMs, "GET", k).Text()
}
func (t *emberTarget) History(ctx context.Context, _ int, k string) (int, error) {
	v, err := t.rdb.Do(ctx, "HISTORY", k, "COUNT", 100).Slice()
	return len(v), err
}
func (t *emberTarget) Branch(ctx context.Context) (bool, error) {
	return true, t.rdb.Do(ctx, "BRANCH", "CREATE", "bench").Err()
}
func (t *emberTarget) Close() { t.rdb.Close() }

// ---- etcd: Get WithRev(revision) ----

type etcdTarget struct {
	endpoint string
	cli      *clientv3.Client
	cpRev    int64
}

func (t *etcdTarget) Name() string { return "etcd" }
func (t *etcdTarget) Notes() map[string]string {
	return map[string]string{
		"read_past": "Get(key, WithRev(revision at checkpoint))",
		"history":   "no history call: walked back one Get per version via ModRevision-1",
		"branch":    "not supported",
	}
}
func (t *etcdTarget) Setup(ctx context.Context, _ int) error {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{t.endpoint}, DialTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	t.cli = cli
	_, err = cli.Get(ctx, "ping")
	return err
}
func (t *etcdTarget) Set(ctx context.Context, _ int, k, v string) error {
	_, err := t.cli.Put(ctx, k, v)
	return err
}
func (t *etcdTarget) Get(ctx context.Context, _ int, k string) (string, error) {
	return t.getRev(ctx, k, 0)
}
func (t *etcdTarget) getRev(ctx context.Context, k string, rev int64) (string, error) {
	var opts []clientv3.OpOption
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev))
	}
	r, err := t.cli.Get(ctx, k, opts...)
	if err != nil || len(r.Kvs) == 0 {
		return "", err
	}
	return string(r.Kvs[0].Value), nil
}
func (t *etcdTarget) Checkpoint(ctx context.Context, _ int) error {
	r, err := t.cli.Get(ctx, "checkpoint")
	if err == nil {
		t.cpRev = r.Header.Revision
	}
	return err
}
func (t *etcdTarget) GetAtCheckpoint(ctx context.Context, _ int, k string) (string, error) {
	return t.getRev(ctx, k, t.cpRev)
}
func (t *etcdTarget) History(ctx context.Context, _ int, k string) (int, error) {
	n := 0
	var rev int64
	for {
		var opts []clientv3.OpOption
		if rev > 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		r, err := t.cli.Get(ctx, k, opts...)
		if err != nil {
			return n, err
		}
		if len(r.Kvs) == 0 {
			return n, nil
		}
		n++
		rev = r.Kvs[0].ModRevision - 1
		if rev <= 0 {
			return n, nil
		}
	}
}
func (t *etcdTarget) Branch(context.Context) (bool, error) { return false, nil }
func (t *etcdTarget) Close()                               { t.cli.Close() }

// ---- immudb: GetAtRevision, History ----

type immuTarget struct {
	host    string
	port    int
	clients []immudb.ImmuClient
	cpRev   int64
}

func (t *immuTarget) Name() string { return "immudb" }
func (t *immuTarget) Notes() map[string]string {
	return map[string]string{
		"read_past": "GetAtRevision(key, n): immudb reads the past per key revision, not per point in time",
		"history":   "History(key, limit 100)",
		"branch":    "not supported",
	}
}
func (t *immuTarget) Setup(ctx context.Context, workers int) error {
	for i := 0; i < workers; i++ {
		c := immudb.NewClient().WithOptions(immudb.DefaultOptions().WithAddress(t.host).WithPort(t.port))
		if err := c.OpenSession(ctx, []byte("immudb"), []byte("immudb"), "defaultdb"); err != nil {
			return err
		}
		t.clients = append(t.clients, c)
	}
	return nil
}
func (t *immuTarget) Set(ctx context.Context, w int, k, v string) error {
	_, err := t.clients[w].Set(ctx, []byte(k), []byte(v))
	return err
}
func (t *immuTarget) Get(ctx context.Context, w int, k string) (string, error) {
	e, err := t.clients[w].Get(ctx, []byte(k))
	if err != nil {
		return "", err
	}
	return string(e.Value), nil
}
func (t *immuTarget) Checkpoint(_ context.Context, round int) error {
	t.cpRev = int64(round) // every key was written exactly once per round
	return nil
}
func (t *immuTarget) GetAtCheckpoint(ctx context.Context, w int, k string) (string, error) {
	e, err := t.clients[w].GetAtRevision(ctx, []byte(k), t.cpRev)
	if err != nil {
		return "", err
	}
	return string(e.Value), nil
}
func (t *immuTarget) History(ctx context.Context, w int, k string) (int, error) {
	es, err := t.clients[w].History(ctx, &schema.HistoryRequest{Key: []byte(k), Limit: 100, Desc: true})
	if err != nil {
		return 0, err
	}
	return len(es.Entries), nil
}
func (t *immuTarget) Branch(context.Context) (bool, error) { return false, nil }
func (t *immuTarget) Close() {
	for _, c := range t.clients {
		c.CloseSession(context.Background())
	}
}

// ---- Dolt: SQL over the MySQL protocol, AS OF <commit>, dolt_history ----

var doltHash = regexp.MustCompile(`^[0-9a-v]{32}$`)

type doltTarget struct {
	dsn       string
	db        *sql.DB
	cp        string
	finalOnce sync.Once
	finalErr  error
}

func (t *doltTarget) Name() string { return "dolt" }
func (t *doltTarget) Notes() map[string]string {
	return map[string]string{
		"read_past": "SELECT ... AS OF '<commit>': only Dolt commits (CALL DOLT_COMMIT) are kept as history",
		"history":   "dolt_history_kv: one version per Dolt commit, not per write",
		"branch":    "CALL DOLT_BRANCH",
	}
}
func (t *doltTarget) Setup(ctx context.Context, workers int) error {
	boot, err := sql.Open("mysql", t.dsn)
	if err != nil {
		return err
	}
	if _, err := boot.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS bench"); err != nil {
		return err
	}
	boot.Close()
	t.db, err = sql.Open("mysql", t.dsn+"bench")
	if err != nil {
		return err
	}
	t.db.SetMaxOpenConns(workers)
	t.db.SetMaxIdleConns(workers)
	_, err = t.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS kv (k VARCHAR(64) PRIMARY KEY, v VARCHAR(256))")
	return err
}
func (t *doltTarget) Set(ctx context.Context, _ int, k, v string) error {
	_, err := t.db.ExecContext(ctx, "INSERT INTO kv (k, v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v = VALUES(v)", k, v)
	return err
}
func (t *doltTarget) Get(ctx context.Context, _ int, k string) (string, error) {
	var v string
	err := t.db.QueryRowContext(ctx, "SELECT v FROM kv WHERE k = ?", k).Scan(&v)
	return v, err
}
func (t *doltTarget) commit(ctx context.Context, msg string) (string, error) {
	var hash string
	err := t.db.QueryRowContext(ctx, "CALL DOLT_COMMIT('-Am', ?)", msg).Scan(&hash)
	return hash, err
}
func (t *doltTarget) Checkpoint(ctx context.Context, _ int) (err error) {
	t.cp, err = t.commit(ctx, "checkpoint")
	return err
}
func (t *doltTarget) GetAtCheckpoint(ctx context.Context, _ int, k string) (string, error) {
	if !doltHash.MatchString(t.cp) { // it is spliced into the query below
		return "", errors.New("unexpected commit hash " + t.cp)
	}
	var v string
	err := t.db.QueryRowContext(ctx, "SELECT v FROM kv AS OF '"+t.cp+"' WHERE k = ?", k).Scan(&v)
	return v, err
}
func (t *doltTarget) History(ctx context.Context, _ int, k string) (int, error) {
	// Commit the final state too (once, whichever worker gets here first),
	// so history holds the checkpoint and the end state.
	t.finalOnce.Do(func() { _, t.finalErr = t.commit(ctx, "final") })
	if t.finalErr != nil {
		return 0, t.finalErr
	}
	var n int
	err := t.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dolt_history_kv WHERE k = ?", k).Scan(&n)
	return n, err
}
func (t *doltTarget) Branch(ctx context.Context) (bool, error) {
	_, err := t.db.ExecContext(ctx, "CALL DOLT_BRANCH('bench')")
	return true, err
}
func (t *doltTarget) Close() { t.db.Close() }
