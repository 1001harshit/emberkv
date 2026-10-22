// Command emberkv runs the emberkv database server.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

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
	trigger := flag.Int("compaction-trigger", 4, "tables per tier before they are merged")
	version := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *version {
		fmt.Println("emberkv", server.Version)
		return
	}

	logger := log.New(os.Stderr, "emberkv ", log.LstdFlags)
	mode, err := wal.ParseSyncMode(*fsync)
	if err != nil {
		logger.Fatal(err)
	}

	st, err := keyspace.Open(*dir, lsm.Options{
		MemtableSize:      *memtableMB << 20,
		SyncMode:          mode,
		CompactionTrigger: *trigger,
		Logger:            logger,
	})
	if err != nil {
		logger.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		st.Close()
		logger.Fatal(err)
	}
	srv := server.New(st, logger)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Print("shutting down")
		srv.Close()
	}()

	logger.Printf("listening on %s (data dir %q, fsync %s)", ln.Addr(), *dir, mode)
	if err := srv.Serve(ln); err != nil {
		logger.Print(err)
	}
	if err := st.Close(); err != nil {
		logger.Fatal(err)
	}
}
