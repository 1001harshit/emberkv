// Package server exposes a keyspace.Store over TCP using the Redis protocol,
// so redis-cli and ordinary Redis client libraries can talk to it.
package server

import (
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1001harshit/emberkv/internal/resp"
	"github.com/1001harshit/emberkv/keyspace"
)

// Version is reported by INFO and -version. Release builds set it with
// -ldflags "-X github.com/1001harshit/emberkv/server.Version=...".
var Version = "dev"

// Server serves Redis-protocol clients from one database.
type Server struct {
	st      *keyspace.Store
	logger  *log.Logger
	started time.Time
	cursors *cursorTable

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	totalConns atomic.Uint64
	totalCmds  atomic.Uint64
}

// New returns a server for st. logger may be nil.
func New(st *keyspace.Store, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return &Server{
		st:      st,
		logger:  logger,
		started: time.Now(),
		cursors: newCursorTable(4096),
		conns:   make(map[net.Conn]struct{}),
	}
}

// ListenAndServe listens on addr and serves until Close is called.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return net.ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		s.totalConns.Add(1)
		go s.handle(conn)
	}
}

// Addr returns the listening address, or nil before Serve.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Close stops accepting connections, closes open ones and waits for their
// handlers to return. It does not close the database.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	// Writes made by this connection are collected in pending instead of
	// each waiting for the disk. Before replies are sent, one wait makes the
	// whole batch durable, so a pipeline of N writes costs one log write
	// while no reply ever reports a write that could still be lost.
	var pending keyspace.Pending
	st := s.st.Session(&pending)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				w.WriteError("ERR " + err.Error())
				w.Flush()
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		s.totalCmds.Add(1)
		quit := s.dispatch(st, w, args)
		// Reply only when no pipelined commands are waiting, so a pipeline of
		// N commands costs one log write and one network write instead of N.
		if quit || r.Buffered() == 0 {
			if err := pending.Wait(); err != nil {
				// The writes may not be durable: drop the connection rather
				// than send replies that claim they are.
				s.logger.Printf("closing connection: %v", err)
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
		}
		if quit {
			return
		}
	}
}

func (s *Server) dispatch(st *keyspace.Store, w *resp.Writer, args [][]byte) (quit bool) {
	name := strings.ToLower(string(args[0]))
	cmd, ok := commands[name]
	if !ok {
		w.WriteError(unknownCommandError(args))
		return false
	}
	if !cmd.arityOK(len(args)) {
		w.WriteError("ERR wrong number of arguments for '" + name + "' command")
		return false
	}
	c := &ctx{s: s, st: st, w: w, args: args}
	cmd.fn(c)
	return c.quit
}

// unknownCommandError formats the error exactly as Redis does, since some
// clients match on it: the name is cut at 128 bytes, and the quoted
// arguments are listed until they reach 128 bytes.
func unknownCommandError(args [][]byte) string {
	var list []byte
	for _, a := range args[1:] {
		if len(list) >= 128 {
			break
		}
		list = append(list, '\'')
		list = append(list, a[:min(len(a), 128-len(list)+1)]...)
		list = append(list, '\'', ' ')
	}
	name := args[0][:min(len(args[0]), 128)]
	return "ERR unknown command '" + string(name) + "', with args beginning with: " + string(list)
}

// cursorTable maps the integer cursors SCAN hands to clients onto the key the
// next page starts from. Redis clients expect integer cursors, while an LSM
// scan naturally resumes from a key.
type cursorTable struct {
	mu    sync.Mutex
	next  uint64
	keys  map[uint64][]byte
	order []uint64
	limit int
}

func newCursorTable(limit int) *cursorTable {
	return &cursorTable{next: 1, keys: make(map[uint64][]byte), limit: limit}
}

func (t *cursorTable) put(key []byte) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.order) >= t.limit {
		delete(t.keys, t.order[0])
		t.order = t.order[1:]
	}
	id := t.next
	t.next++
	t.keys[id] = key
	t.order = append(t.order, id)
	return id
}

func (t *cursorTable) get(id uint64) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k, ok := t.keys[id]
	return k, ok
}
