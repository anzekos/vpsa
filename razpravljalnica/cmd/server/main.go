package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "razpravljalnica/proto"
	"razpravljalnica/server"
)

func main() {
	// Command line flags (use single dash for Windows compatibility)
	port := flag.Int("port", 50051, "Server port")
	nodeID := flag.String("id", "node-1", "Node ID")
	flag.Parse()

	address := fmt.Sprintf("localhost:%d", *port)

	// Ustvari server
	srv := server.NewServer(*nodeID, address)

	// Ustvari gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, srv)
	pb.RegisterControlPlaneServer(grpcServer, srv)

	// Zaženi server
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	fmt.Printf("🚀 Razpravljalnica server starting on %s (Node ID: %s)\n", address, *nodeID)
	fmt.Println("=")
	fmt.Println("Server is ready to accept connections!")
	fmt.Println("=")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
