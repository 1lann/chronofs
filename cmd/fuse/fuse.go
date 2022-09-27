package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"strconv"
	"time"

	"github.com/1lann/chronofs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

var pprofPort = flag.Int("pprof", 0, "Port to bind a pprof HTTP server on. Set to 0 to disable.")
var logFile = flag.String("log", "", "Log file to write to. Disabled by default.")
var cacheBytes = flag.Int("cache-bytes", 500e6, "Number of bytes to cache file page data in memory.")
var cacheFiles = flag.Int("cache-files", 50000, "Number of files to cache metadata in memory.")
var syncPeriod = flag.Duration("period", 2*time.Second, "Period to sync the database.")

func main() {
	flag.Parse()

	if len(flag.Args()) < 2 {
		log.Fatalln("Usage: fuse <database> <mountpoint>")
	}

	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalln(err)
		}
		log.SetOutput(io.MultiWriter(os.Stderr, f))

		defer f.Close()
	}

	ctx := context.Background()

	q := url.Values{}
	q.Set("cache", "shared")
	q.Add("_pragma", "page_size=65536")
	q.Add("_pragma", "busy_timeout=5000")
	q.Add("_pragma", "synchronous=NORMAL")
	q.Add("_pragma", "journal_mode=WAL")
	q.Add("_pragma", "wal_autocheckpoint=0")

	u := url.URL{
		Scheme:   "file",
		Opaque:   flag.Arg(0),
		RawQuery: q.Encode(),
	}

	db, err := sqlx.ConnectContext(ctx, "sqlite", u.String())
	if err != nil {
		log.Fatalln(err)
	}

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(-1)
	db.SetConnMaxLifetime(-1)

	timeout := time.Second
	opts := &fs.Options{
		AttrTimeout:  &timeout,
		EntryTimeout: &timeout,
		Logger:       log.Default(),
	}

	opts.MountOptions.Debug = false
	opts.MountOptions.DisableXAttrs = true

	currentUser, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}

	client := chronofs.NewSQLBackedClient(uint64(*cacheFiles), uint64(*cacheBytes), currentUser, db, 18)

	syncCtx, syncCancel := context.WithCancel(context.Background())
	syncDone := make(chan struct{})

	go func() {
		t := time.NewTicker(syncPeriod)

		defer func() {
			t.Stop()
			close(syncDone)
		}()

		for {
			select {
			case <-syncCtx.Done():
				return
			case <-t.C:
			}

			ctx, cancel := context.WithTimeout(syncCtx, time.Second*10)
			t := time.Now()
			// log.Println("starting sync")
			err := client.Sync(ctx)
			cancel()
			if err != nil {
				log.Println("sync error:", err)
			} else {
				log.Println("sync completed in", time.Since(t))
			}
		}
	}()

	server, err := fs.Mount(flag.Arg(1), chronofs.NewRootNode(client, currentUser), opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	if *pprofPort != 0 {
		port := strconv.Itoa(*pprofPort)
		go func() {
			log.Println("running pprof server on 127.0.0.1:" + port)
			err := http.ListenAndServe("127.0.0.1:"+port, nil)
			if err != nil {
				log.Println("pprof server error:", err)
			}
		}()
	}

	log.Println("mounted, FUSE server is running, ctrl+c to unmount")

	defer func() {
		server.Unmount()
	}()

	termination := make(chan struct{})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Println("ctrl+c received, unmounting...")
			syncCancel()
			if err := server.Unmount(); err != nil {
				log.Println("unmount error:", err)
				log.Println("you may retry unmounting by sending ctrl+c again")
			} else {
				log.Println("finishing up sync...")
				<-syncDone

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cancel()

				if err := client.Sync(ctx, true); err != nil {
					log.Println("graceful termination error:", err)
				}

				close(termination)

				return
			}
		}
	}()

	server.Wait()
	<-termination

	log.Println("gracefully terminated")
}
