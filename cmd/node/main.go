// cmd/node/main.go - Zagon Node-a
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"razpravljalnica/node"
	"razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9001", "Node address")
	masterAddr := flag.String("master", "localhost:9000", "Master address")
	flag.Parse()

	n := node.NewNode(*addr)

	// Start node
	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Register with master
	log.Printf("Registering with master at %s", *masterAddr)
	conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}

	// In real implementation, we'd have a RegisterNode RPC
	// For now, master's AddNode is called manually or via separate admin tool
	log.Printf("Node started at %s (manual registration required)", *addr)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutting down node...")
	n.Stop()
}
