// cmd/admin/main.go - Admin tool za dodajanje vozlišč
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	masterAddr := flag.String("master", "localhost:9000", "Master address")
	nodeAddr := flag.String("node", "", "Node address to add")
	nodeID := flag.String("id", "", "Node ID")
	flag.Parse()

	if *nodeAddr == "" || *nodeID == "" {
		log.Fatal("Both -node and -id are required")
	}

	// Connect to master
	conn, err := grpc.Dial(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	defer conn.Close()

	// Note: This requires adding a RegisterNode RPC to ControlPlane service
	// For now, this is a placeholder showing the concept

	log.Printf("Adding node %s (%s) to cluster...", *nodeID, *nodeAddr)
	
	// In complete implementation, call master.AddNode via RPC
	// masterClient := pb.NewControlPlaneClient(conn)
	// resp, err := masterClient.RegisterNode(ctx, &pb.RegisterNodeRequest{...})

	log.Println("Node added successfully")
}
