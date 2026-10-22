package server

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/1001harshit/emberkv/keyspace"
	"github.com/1001harshit/emberkv/lsm"
)

// startWithBranches starts a server with branches enabled in dir and
// returns a function that shuts everything down.
func startWithBranches(t *testing.T, dir string, opts lsm.Options) (string, func()) {
	t.Helper()
	st, err := keyspace.Open(filepath.Join(dir, "main"), opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := OpenBranches(st, filepath.Join(dir, "main", "branches"), func(p string) (*keyspace.Store, error) {
		return keyspace.Open(p, opts)
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(st, nil)
	s.EnableBranches(b)
	go s.Serve(ln)
	return ln.Addr().String(), func() {
		s.Close()
		b.Close()
		st.Close()
	}
}

func TestBranches(t *testing.T) {
	dir := t.TempDir()
	opts := lsm.Options{History: time.Hour}
	addr, stop := startWithBranches(t, dir, opts)
	c := dial(t, addr)

	c.do("SET", "plan", "free")
	c.do("HSET", "user:1", "name", "ada")
	c.expect("main", "BRANCH", "CURRENT")
	c.expect("OK", "BRANCH", "CREATE", "experiment")
	c.expectErr("ERR branch \"experiment\" already exists", "BRANCH", "CREATE", "experiment")
	c.expectErr("ERR invalid branch name", "BRANCH", "CREATE", "../escape")
	c.expect(list("main", "experiment"), "BRANCH", "LIST")

	c.expect("OK", "BRANCH", "USE", "experiment")
	c.expect("experiment", "BRANCH", "CURRENT")
	c.expect("free", "GET", "plan")
	c.do("SET", "plan", "pro")
	c.do("HSET", "user:1", "beta", "yes")
	c.do("DEL", "user:1")

	other := dial(t, addr) // a second client stays on main
	other.expect("free", "GET", "plan")
	other.expect(list("name", "ada"), "HGETALL", "user:1")
	c.expect("pro", "GET", "plan")
	c.expect(int64(0), "EXISTS", "user:1")

	// Branches of branches, and branches of the past.
	c.expect("OK", "BRANCH", "CREATE", "experiment-2")
	c.expect("OK", "BRANCH", "USE", "experiment-2")
	c.expect("pro", "GET", "plan")
	c.expect("OK", "BRANCH", "USE", "main")
	beforeUpgrade := time.Now()
	time.Sleep(20 * time.Millisecond)
	c.do("SET", "plan", "enterprise")
	c.expect("OK", "BRANCH", "CREATE", "before", "ASOF", fmt.Sprint(beforeUpgrade.UnixMilli()))
	c.expect("OK", "BRANCH", "USE", "before")
	c.expect("free", "GET", "plan")
	c.expect("OK", "BRANCH", "USE", "main")

	c.expectErr("ERR can't drop the branch this connection is using", "BRANCH", "DROP", "main")
	c.expect("OK", "BRANCH", "DROP", "experiment-2")
	c.expectErr("ERR no branch named", "BRANCH", "USE", "experiment-2")

	// Branches survive a restart.
	stop()
	addr, stop = startWithBranches(t, dir, opts)
	defer stop()
	c = dial(t, addr)
	c.expect(list("main", "before", "experiment"), "BRANCH", "LIST")
	c.expect("enterprise", "GET", "plan")
	c.expect("OK", "BRANCH", "USE", "experiment")
	c.expect("pro", "GET", "plan")
}

func TestBranchesDisabled(t *testing.T) {
	c := newClient(t)
	c.expectErr("ERR branches are not enabled", "BRANCH", "LIST")
}
