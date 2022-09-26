package main

import (
	"context"
	"io"
	"io/fs"
	"log"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/1lann/chronofs"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// extracts a folder from a sqlite database

func main() {
	start := time.Now()

	if len(os.Args) < 3 {
		log.Fatalln("Usage: extract <database> <folder>")
	}

	u := url.URL{
		Scheme:   "file",
		Opaque:   os.Args[1],
		RawQuery: "cache=shared&mode=ro",
	}

	ctx := context.Background()
	db, err := sqlx.ConnectContext(ctx, "sqlite", u.String())
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

	usr, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}

	client := chronofs.NewSQLBackedClient(10000, 1e7, usr, db, 18)

	fsys := chronofs.NewRootStdFS(client)

	// entries, err := fsys.ReadDir("/")
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// for _, entry := range entries {
	// 	log.Println(entry.Name())
	// }

	log.Println("setup took:", time.Since(start))

	start = time.Now()

	err = fs.WalkDir(fsys, "/world", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		dst := filepath.Join(os.Args[2], strings.TrimPrefix(p, "/world"))

		finfo, err := d.Info()
		if err != nil {
			return err
		}

		if d.IsDir() {
			return os.MkdirAll(dst, finfo.Mode().Perm())
		}

		f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, finfo.Mode().Perm())
		if err != nil {
			return err
		}

		defer f.Close()

		vf, err := fsys.Open(p)
		if err != nil {
			return err
		}

		defer vf.Close()

		_, err = vf.(io.WriterTo).WriteTo(f)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("extract took:", time.Since(start))
}
