package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"os/user"
	"time"

	"github.com/1lann/chronofs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// ExampleLoopbackReuse shows how to build a file system on top of the
// loopback file system.
func main() {
	// mntDir := "/home/jason/Workspace/testserver/serverdata"
	// origDir := "/home/jason/Workspace/testserver/origdata"
	if len(os.Args) < 3 {
		log.Fatalln("Usage: fuse <database> <mountpoint>")
	}

	ctx := context.Background()

	q := url.Values{}
	q.Set("cache", "shared")
	q.Set("_pragma", "busy_timeout(5000)")
	q.Set("_pragma", "synchronous(NORMAL)")
	q.Set("_pragma", "journal_mode(WAL)")
	q.Set("_pragma", "wal_autocheckpoint(0)")
	q.Set("_pragma", "page_size(65536)")

	u := url.URL{
		Scheme:   "file",
		Opaque:   os.Args[1],
		RawQuery: q.Encode(),
	}

	db, err := sqlx.ConnectContext(ctx, "sqlite", u.String())
	if err != nil {
		log.Fatalln(err)
	}

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

	client := chronofs.NewSQLBackedClient(10000, 1e8, currentUser, db, 18)

	go func() {
		t := time.NewTicker(5 * time.Second)

		for range t.C {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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

	server, err := fs.Mount(os.Args[2], chronofs.NewRootNode(client, currentUser), opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	log.Printf("files under %s cannot be deleted if they are opened", os.Args[2])

	server.Wait()
}