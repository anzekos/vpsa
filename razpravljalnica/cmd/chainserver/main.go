package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	pb "razpravljalnica/proto"
	"razpravljalnica/server"
)

func main() {
	// Command line flags
	port := flag.Int("port", 50051, "Server port")
	nodeID := flag.String("id", "node-1", "Node ID")
	controlPlane := flag.String("control", "localhost:50050", "Control plane address")
	flag.Parse()

	address := fmt.Sprintf("localhost:%d", *port)

	// Ustvari chain node - role bo določena s strani control plane
	node := server.NewChainNode(*nodeID, address, server.RoleMiddle, *controlPlane)

	// Ustvari gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterMessageBoardServer(grpcServer, node)
	pb.RegisterReplicationServiceServer(grpcServer, node)
	pb.RegisterControlPlaneServer(grpcServer, node)

	// Zaženi server
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	fmt.Println("=")
	fmt.Printf("🔗 Chain Node starting\n")
	fmt.Printf("   Node ID:       %s\n", *nodeID)
	fmt.Printf("   Address:       %s\n", address)
	fmt.Printf("   Control Plane: %s\n", *controlPlane)
	fmt.Println("=")

	// Registriraj v control plane (v ločenem threadu)
	go func() {
		time.Sleep(500 * time.Millisecond) // Počakaj da server zažene
		if err := node.RegisterWithControlPlane(); err != nil {
			log.Printf("⚠️  Failed to register with control plane: %v", err)
			log.Printf("Node will continue without control plane management")
		}
	}()

	fmt.Println("Server is ready!")
	fmt.Println("=")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
