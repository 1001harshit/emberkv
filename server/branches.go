package server

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/1001harshit/emberkv/keyspace"
)

// MainBranch is the name of the database the server was started with.
const MainBranch = "main"

var branchName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Branches manages named databases that start as instant copies of another
// one (see lsm.DB.Checkpoint). Each lives in its own directory under dir and
// is reopened on restart.
type Branches struct {
	mu     sync.Mutex
	dir    string
	open   func(dir string) (*keyspace.Store, error)
	stores map[string]*keyspace.Store
}

// OpenBranches opens every branch found in dir, which is created if needed.
// open opens a branch directory with the server's options.
func OpenBranches(main *keyspace.Store, dir string, open func(dir string) (*keyspace.Store, error)) (*Branches, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Branches{dir: dir, open: open, stores: map[string]*keyspace.Store{MainBranch: main}}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if !e.IsDir() || !branchName.MatchString(e.Name()) || e.Name() == MainBranch {
			continue
		}
		st, err := open(filepath.Join(dir, e.Name()))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("opening branch %q: %w", e.Name(), err)
		}
		b.stores[e.Name()] = st
	}
	return b, nil
}

// Get returns the named branch.
func (b *Branches) Get(name string) (*keyspace.Store, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	st, ok := b.stores[name]
	return st, ok
}

// Names returns the branch names, main first.
func (b *Branches) Names() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var names []string
	for n := range b.stores {
		if n != MainBranch {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return append([]string{MainBranch}, names...)
}

// Create makes branch name a copy of from, as it is now or, if at is
// non-zero, as it was then.
func (b *Branches) Create(name string, from *keyspace.Store, at time.Time) error {
	if !branchName.MatchString(name) {
		return replyErr("ERR invalid branch name: use 1-64 letters, digits, '-' or '_'")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.stores[name]; exists {
		return replyErr(fmt.Sprintf("ERR branch %q already exists", name))
	}
	path := filepath.Join(b.dir, name)
	if err := from.DB().Checkpoint(path, at); err != nil {
		return err
	}
	st, err := b.open(path)
	if err != nil {
		os.RemoveAll(path)
		return err
	}
	b.stores[name] = st
	return nil
}

// Drop closes branch name and deletes its data. Connections still using it
// get errors from then on.
func (b *Branches) Drop(name string) error {
	if name == MainBranch {
		return replyErr("ERR the main branch can't be dropped")
	}
	b.mu.Lock()
	st, ok := b.stores[name]
	delete(b.stores, name)
	b.mu.Unlock()
	if !ok {
		return replyErr(fmt.Sprintf("ERR no branch named %q", name))
	}
	st.Close()
	return os.RemoveAll(filepath.Join(b.dir, name))
}

// Close closes every branch except main, which belongs to the caller.
func (b *Branches) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var first error
	for name, st := range b.stores {
		if name == MainBranch {
			continue
		}
		if err := st.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// EnableBranches turns on the BRANCH command.
func (s *Server) EnableBranches(b *Branches) { s.branches = b }

// cmdBranch is an emberkv extension:
//
//	BRANCH CREATE <name> [ASOF <time>]   copy the current branch (now, or as of <time>)
//	BRANCH USE <name>                    switch this connection to a branch
//	BRANCH CURRENT                       the branch this connection uses
//	BRANCH LIST                          all branches, main first
//	BRANCH DROP <name>                   delete a branch
func cmdBranch(c *ctx) {
	b := c.s.branches
	if b == nil {
		c.w.WriteError("ERR branches are not enabled on this server")
		return
	}
	sub := strings.ToUpper(string(c.args[1]))
	switch {
	case sub == "CREATE" && (len(c.args) == 3 || len(c.args) == 5):
		var at time.Time
		if len(c.args) == 5 {
			if !strings.EqualFold(string(c.args[3]), "ASOF") {
				c.fail(errSyntax)
				return
			}
			var ok bool
			if at, ok = c.parseTimeArg(c.args[4]); !ok {
				return
			}
		}
		from, _ := b.Get(c.conn.branch)
		if from == nil {
			c.w.WriteError("ERR the current branch no longer exists")
			return
		}
		if err := b.Create(string(c.args[2]), from, at); err != nil {
			c.fail(err)
			return
		}
		c.ok()
	case sub == "USE" && len(c.args) == 3:
		name := string(c.args[2])
		st, ok := b.Get(name)
		if !ok {
			c.w.WriteError(fmt.Sprintf("ERR no branch named %q", name))
			return
		}
		c.conn.use(name, st)
		c.ok()
	case sub == "CURRENT" && len(c.args) == 2:
		c.w.WriteBulkString(c.conn.branch)
	case sub == "LIST" && len(c.args) == 2:
		names := b.Names()
		c.w.WriteArray(len(names))
		for _, n := range names {
			c.w.WriteBulkString(n)
		}
	case sub == "DROP" && len(c.args) == 3:
		name := string(c.args[2])
		if name == c.conn.branch {
			c.w.WriteError("ERR can't drop the branch this connection is using; BRANCH USE another first")
			return
		}
		if err := b.Drop(name); err != nil {
			c.fail(err)
			return
		}
		c.ok()
	default:
		c.fail(errSyntax)
	}
}
