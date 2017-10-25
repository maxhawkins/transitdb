package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/handlers"

	"github.com/maxhawkins/transitdb"
	"github.com/maxhawkins/transitdb/pg"
)

func main() {
	var (
		dbPath = flag.String("db", "postgres://localhost/transitdb?sslmode=disable", "db location")
		port   = flag.Int("port", 5030, "http port")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	db, err := pg.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var handler http.Handler
	handler = &transitdb.Handler{Store: db}
	handler = handlers.LoggingHandler(os.Stderr, handler)

	addr := fmt.Sprint(":", *port)
	fmt.Fprintln(os.Stderr, "listening at", addr)
	log.Fatal(http.ListenAndServe(addr, handler))
}
