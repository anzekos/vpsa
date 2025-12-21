// cmd/master/main.go - Zagon Master-ja
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"razpravljalnica/master"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "Master address")
	flag.Parse()

	m := master.NewMaster(*addr)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down master...")
		m.Stop()
		os.Exit(0)
	}()

	log.Printf("Starting master on %s", *addr)
	if err := m.Start(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}
}
