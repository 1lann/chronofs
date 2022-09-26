package main

import (
	"context"
	"log"
	"os/user"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// ExampleLoopbackReuse shows how to build a file system on top of the
// loopback file system.
func main() {
	// mntDir := "/home/jason/Workspace/testserver/serverdata"
	// origDir := "/home/jason/Workspace/testserver/origdata"
	mntDir := "./mountpoint/x"

	ctx := context.Background()
	db, err := sqlx.ConnectContext(ctx, "sqlite", "file:./time.db?cache=shared")
	if err != nil {
		log.Fatalln(err)
	}

	for _, pragma := range []string{
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA synchronous = NORMAL;`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA wal_autocheckpoint = 0;`,
		`PRAGMA page_size = 65536;`,
	} {
		_, err = db.ExecContext(ctx, pragma)
		if err != nil {
			log.Fatalln(err)
		}
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

	client := NewSQLBackedClient(10000, 1e8, currentUser, db, 18)

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

	server, err := fs.Mount(mntDir, &Node{
		fileID:   2,
		fileType: FileTypeDirectory,
		client:   client,
	}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	log.Printf("files under %s cannot be deleted if they are opened", mntDir)

	server.Wait()
}
