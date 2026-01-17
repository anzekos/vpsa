package main

import (
	"flag"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Helper tool za registracijo chain nodes na HEAD za subscription routing

func main() {
	headAddr := flag.String("head", "localhost:50051", "HEAD node address")
	nodeID := flag.String("id", "", "Node ID to register")
	nodeAddr := flag.String("addr", "", "Node address to register")
	flag.Parse()

	if *nodeID == "" || *nodeAddr == "" {
		log.Fatal("Both -id and -addr are required")
	}

	// Poveži se na HEAD
	conn, err := grpc.Dial(*headAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to HEAD: %v", err)
	}
	defer conn.Close()

	// Naredimo custom RPC klic za registracijo
	// V produkciji bi to bilo del kontrolne ravnine

	// Za ta primer: HEAD bo že registriran, ostale node moramo dodati ročno
	// To je poenostavitev - v verziji 9-10 bo to avtomatsko

	fmt.Printf("Node registration helper\n")
	fmt.Printf("In version 7-8, manually ensure HEAD knows about all nodes.\n")
	fmt.Printf("Node: %s at %s\n", nodeID, *nodeAddr)
	fmt.Printf("This will be automated in version 9-10 with control plane.\n")
}
