package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"razpravljalnica/controlplane"
	pb "razpravljalnica/proto"
)

func main() {
	port := flag.Int("port", 50050, "Control plane port")
	flag.Parse()

	address := fmt.Sprintf("localhost:%d", *port)

	// Ustvari control plane
	cp := controlplane.NewControlPlane()

	// Ustvari gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterControlPlaneServer(grpcServer, cp)
	pb.RegisterNodeManagementServer(grpcServer, cp)

	// Zaženi server
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	fmt.Println("=")
	fmt.Println("🎛️  CONTROL PLANE")
	fmt.Printf("   Address: %s\n", address)
	fmt.Println("=")
	fmt.Println("Monitoring chain health and managing topology...")
	fmt.Println("=")

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nShutting down control plane...")
		cp.Stop()
		grpcServer.GracefulStop()
		os.Exit(0)
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
