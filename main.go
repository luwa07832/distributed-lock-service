// Command distributed-lock-service serves the HTTP API described in README.md.
package main

import (
	"log"
	"os"

	"github.com/luwa07832/distributed-lock-service/internal/api"
	"github.com/luwa07832/distributed-lock-service/internal/store"
	"github.com/luwa07832/distributed-lock-service/locksvc"
)

func main() {
	address := os.Getenv("ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	databasePath := os.Getenv("DB_PATH")
	if databasePath == "" {
		databasePath = "distributed-lock-service.db"
	}

	st, err := store.Open(databasePath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	lockStates := locksvc.New()

	if err := api.NewRouter(st, api.WithLockStateService(lockStates)).Run(address); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
