// Command emberkv runs the emberkv database server.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/1001harshit/emberkv/internal/wal"
	"github.com/1001harshit/emberkv/keyspace"
	"github.com/1001harshit/emberkv/lsm"
	"github.com/1001harshit/emberkv/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "address to listen on")
	dir := flag.String("dir", "data", "data directory")
	fsync := flag.String("fsync", "everysec", "WAL fsync policy: always, everysec or no")
	memtableMB := flag.Int("memtable-mb", 4, "memtable size in MiB before it is flushed to disk")
	trigger := flag.Int("compaction-trigger", 4, "number of SSTables before background compaction starts")
	cacheMB := flag.Int("block-cache-mb", 64, "memory for caching SSTable blocks, in MiB (0 disables)")
	history := flag.Duration("history", time.Hour, "how far back ASOF, HISTORY and ROLLBACK can reach (0 keeps no history)")
	password := flag.String("requirepass", os.Getenv("EMBERKV_PASSWORD"),
		"require clients to AUTH with this password (default $EMBERKV_PASSWORD; empty: no password)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (PEM); with -tls-key, serve TLS only")
	tlsKey := flag.String("tls-key", "", "TLS private key file (PEM)")
	version := flag.Bool("version", false, "print version and exit")
	cpuProfile := flag.String("cpuprofile", "", "write a CPU profile to this file until shutdown")
	flag.Parse()

	if *version {
		fmt.Println("emberkv", server.Version)
		return
	}

	logger := log.New(os.Stderr, "emberkv ", log.LstdFlags)
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			logger.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	mode, err := wal.ParseSyncMode(*fsync)
	if err != nil {
		logger.Fatal(err)
	}

	opts := lsm.Options{
		MemtableSize:      *memtableMB << 20,
		SyncMode:          mode,
		CompactionTrigger: *trigger,
		BlockCacheSize:    blockCacheBytes(*cacheMB),
		History:           *history,
		Logger:            logger,
	}
	st, err := keyspace.Open(*dir, opts)
	if err != nil {
		logger.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		st.Close()
		logger.Fatal(err)
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		logger.Fatal("-tls-cert and -tls-key must be given together")
	}
	if *tlsCert != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			logger.Fatal(err)
		}
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	}
	if *password == "" && !isLoopback(ln.Addr()) {
		logger.Printf("WARNING: listening on %s with no password: anyone who can reach it can read and change all data (set -requirepass)", ln.Addr())
	}
	srv := server.New(st, logger)
	srv.RequirePassword(*password)
	branches, err := server.OpenBranches(st, filepath.Join(*dir, "branches"), func(path string) (*keyspace.Store, error) {
		return keyspace.Open(path, opts)
	})
	if err != nil {
		st.Close()
		logger.Fatal(err)
	}
	srv.EnableBranches(branches)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Print("shutting down")
		srv.Close()
	}()

	logger.Printf("listening on %s (data dir %q, fsync %s, history %v, tls %v, password %v)",
		ln.Addr(), *dir, mode, *history, *tlsCert != "", *password != "")
	if err := srv.Serve(ln); err != nil {
		logger.Print(err)
	}
	if err := branches.Close(); err != nil {
		logger.Print(err)
	}
	if err := st.Close(); err != nil {
		logger.Fatal(err)
	}
}

func isLoopback(a net.Addr) bool {
	tcp, ok := a.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

// blockCacheBytes converts the -block-cache-mb flag to lsm.Options, where 0
// means "use the default" and a negative size disables the cache.
func blockCacheBytes(mb int) int64 {
	if mb <= 0 {
		return -1
	}
	return int64(mb) << 20
}
