package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk"
)

func main() {
	address := flag.String("http-address", ":8080", "HTTP listen address")
	flag.Parse()

	webUI, err := opensplunk.WebUI()
	if err != nil {
		log.Fatalf("open embedded web UI: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.Handle("/", http.FileServerFS(webUI))

	server := &http.Server{
		Addr:              *address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("open-splunk server listening on %s", *address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}
