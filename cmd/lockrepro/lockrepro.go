package main

import (
	"context"
	"flag"
	"log"
	_ "net/http/pprof"
	"net/url"
	"time"

	"github.com/1lann/chronofs/store"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

func main() {
	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Fatalln("Usage: fuse <database> <mountpoint>")
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

	d := store.New(db)
	t := time.NewTicker(time.Second)
	for range t.C {
		start := time.Now()
		err := d.UpsertPage(context.Background(), store.UpsertPageParams{FileID: 1, PageNum: 1, Data: []byte("hello")})
		log.Println("took:", time.Since(start))
		if err != nil {
			log.Println(err)
		}
	}
}
