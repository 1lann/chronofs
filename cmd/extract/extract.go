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

	q := url.Values{}
	q.Set("cache", "shared")
	q.Set("mode", "ro")
	q.Add("_pragma", "page_size=65536")
	q.Add("_pragma", "busy_timeout=5000")

	u := url.URL{
		Scheme:   "file",
		Opaque:   os.Args[1],
		RawQuery: q.Encode(),
	}

	ctx := context.Background()
	db, err := sqlx.ConnectContext(ctx, "sqlite", u.String())
	if err != nil {
		log.Fatalln(err)
	}

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(-1)
	db.SetConnMaxLifetime(-1)

	usr, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}

	client := chronofs.NewSQLBackedClient(10000, 1e7, usr, db, 18)

	fsys := chronofs.NewRootStdFS(client)

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
