// cmd/client/main.go - Zagon odjemalca
package main

import (
	"flag"
	"log"

	"razpravljalnica/client"
)

func main() {
	masterAddr := flag.String("master", "localhost:9000", "Master address")
	interactive := flag.Bool("i", false, "Interactive mode")
	flag.Parse()

	c, err := client.NewClient(*masterAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	if *interactive {
		c.RunInteractive()
	} else {
		// Demo scenario
		runDemo(c)
	}
}

func runDemo(c *client.Client) {
	log.Println("\n=== Running Demo ===\n")

	// Create users
	user1, _ := c.CreateUser("Alice")
	user2, _ := c.CreateUser("Bob")
	log.Printf("Created users: %s, %s\n", user1.Name, user2.Name)

	// Create topics
	topic1, _ := c.CreateTopic("Go Programming")
	topic2, _ := c.CreateTopic("Distributed Systems")
	log.Printf("Created topics: %s, %s\n", topic1.Name, topic2.Name)

	// List topics
	topics, _ := c.ListTopics()
	log.Println("\nAll topics:")
	for _, t := range topics {
		log.Printf("  [%d] %s\n", t.Id, t.Name)
	}

	// Post messages
	msg1, _ := c.PostMessage(topic1.Id, "Hello from Alice!")
	msg2, _ := c.PostMessage(topic1.Id, "Hi Alice, Bob here!")
	log.Printf("\nPosted messages: %d, %d\n", msg1.Id, msg2.Id)

	// View messages
	messages, _ := c.GetMessages(topic1.Id, 0, 10)
	log.Printf("\nMessages in '%s':\n", topic1.Name)
	for _, m := range messages {
		log.Printf("  [%d] User %d: %s (likes: %d)\n",
			m.Id, m.UserId, m.Text, m.Likes)
	}

	// Like messages
	c.LikeMessage(topic1.Id, msg1.Id)
	c.LikeMessage(topic1.Id, msg1.Id) // Should only count once

	// View updated messages
	messages, _ = c.GetMessages(topic1.Id, 0, 10)
	log.Printf("\nAfter likes:\n")
	for _, m := range messages {
		log.Printf("  [%d] User %d: %s (likes: %d)\n",
			m.Id, m.UserId, m.Text, m.Likes)
	}

	log.Println("\n=== Demo Complete ===")
}
