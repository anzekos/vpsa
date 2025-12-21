// cmd/test/main.go - Test failure recovery
package main

import (
	"flag"
	"log"
	"time"

	"razpravljalnica/client"
)

func main() {
	masterAddr := flag.String("master", "localhost:9000", "Master address")
	flag.Parse()

	log.Println("=== Testing Chain Replication Failure Recovery ===\n")

	c, err := client.NewClient(*masterAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create initial data
	user, _ := c.CreateUser("TestUser")
	topic, _ := c.CreateTopic("TestTopic")

	log.Println("Phase 1: Normal operation")
	for i := 0; i < 5; i++ {
		msg, err := c.PostMessage(topic.Id, "Message before failure")
		if err != nil {
			log.Printf("Error: %v", err)
		} else {
			log.Printf("Posted message %d", msg.Id)
		}
		time.Sleep(1 * time.Second)
	}

	log.Println("\n>>> Now kill one node and observe recovery <<<\n")
	time.Sleep(15 * time.Second)

	log.Println("Phase 2: After recovery")
	for i := 0; i < 5; i++ {
		msg, err := c.PostMessage(topic.Id, "Message after recovery")
		if err != nil {
			log.Printf("Error: %v", err)
		} else {
			log.Printf("Posted message %d", msg.Id)
		}
		time.Sleep(1 * time.Second)
	}

	// Verify all messages are there
	messages, _ := c.GetMessages(topic.Id, 0, 100)
	log.Printf("\nTotal messages: %d", len(messages))
	log.Println("=== Test Complete ===")
}
