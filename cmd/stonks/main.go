package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/xd-dash/stonks/router"
)

func main() {
	addr := strings.TrimSpace(os.Getenv("STONKS_LISTEN_ADDR"))
	if addr == "" {
		port := strings.TrimSpace(os.Getenv("PORT"))
		if port == "" {
			port = "8080"
		}
		addr = "127.0.0.1:" + port
	}
	log.Printf("stonks: listening on %s", addr)
	if err := http.ListenAndServe(addr, router.NewRouter()); err != nil {
		log.Fatal(err)
	}
}
