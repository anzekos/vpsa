package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"razpravljalnica/client"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "Server address")
	flag.Parse()

	// Poveži se na server
	c, err := client.NewClient(*serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer c.Close()

	fmt.Println("🎯 Razpravljalnica Client")
	fmt.Println("Connected to:", *serverAddr)
	fmt.Println("=")

	// Interaktivni CLI
	runInteractiveCLI(c)
}

func runInteractiveCLI(c *client.Client) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("\nAvailable commands:")
	fmt.Println("  create-user <name>")
	fmt.Println("  create-topic <name>")
	fmt.Println("  post <topic_id> <user_id> <text>")
	fmt.Println("  like <topic_id> <message_id> <user_id>")
	fmt.Println("  list-topics")
	fmt.Println("  get-messages <topic_id> [from_id] [limit]")
	fmt.Println("  subscribe <user_id> <topic_id1,topic_id2,...> [from_id]")
	fmt.Println("  cluster-state")
	fmt.Println("  help")
	fmt.Println("  exit")
	fmt.Println()

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "exit", "quit":
			fmt.Println("Goodbye!")
			return

		case "help":
			showHelp()

		case "create-user":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: create-user <name>")
				continue
			}
			name := strings.Join(parts[1:], " ")
			c.CreateUser(name)

		case "create-topic":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: create-topic <name>")
				continue
			}
			name := strings.Join(parts[1:], " ")
			c.CreateTopic(name)

		case "post":
			if len(parts) < 4 {
				fmt.Println("❌ Usage: post <topic_id> <user_id> <text>")
				continue
			}
			topicID, _ := strconv.ParseInt(parts[1], 10, 64)
			userID, _ := strconv.ParseInt(parts[2], 10, 64)
			text := strings.Join(parts[3:], " ")
			c.PostMessage(topicID, userID, text)

		case "like":
			if len(parts) < 4 {
				fmt.Println("❌ Usage: like <topic_id> <message_id> <user_id>")
				continue
			}
			topicID, _ := strconv.ParseInt(parts[1], 10, 64)
			messageID, _ := strconv.ParseInt(parts[2], 10, 64)
			userID, _ := strconv.ParseInt(parts[3], 10, 64)
			c.LikeMessage(topicID, messageID, userID)

		case "list-topics":
			c.ListTopics()

		case "get-messages":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: get-messages <topic_id> [from_id] [limit]")
				continue
			}
			topicID, _ := strconv.ParseInt(parts[1], 10, 64)
			fromID := int64(0)
			limit := int32(100)
			if len(parts) > 2 {
				fromID, _ = strconv.ParseInt(parts[2], 10, 64)
			}
			if len(parts) > 3 {
				l, _ := strconv.ParseInt(parts[3], 10, 32)
				limit = int32(l)
			}
			c.GetMessages(topicID, fromID, limit)

		case "subscribe":
			if len(parts) < 3 {
				fmt.Println("❌ Usage: subscribe <user_id> <topic_id1,topic_id2,...> [from_id]")
				continue
			}
			userID, _ := strconv.ParseInt(parts[1], 10, 64)

			// Parse topic IDs
			topicIDStrs := strings.Split(parts[2], ",")
			topicIDs := make([]int64, 0)
			for _, tidStr := range topicIDStrs {
				tid, _ := strconv.ParseInt(strings.TrimSpace(tidStr), 10, 64)
				topicIDs = append(topicIDs, tid)
			}

			fromID := int64(0)
			if len(parts) > 3 {
				fromID, _ = strconv.ParseInt(parts[3], 10, 64)
			}

			// Subscribe je blocking, zato uporabnik ne bo mogel več vpisovati
			fmt.Println("⚠️  Subscription is blocking. Press Ctrl+C to stop.")
			err := c.SubscribeTopic(userID, topicIDs, fromID)
			if err != nil {
				fmt.Printf("❌ Subscription error: %v\n", err)
			}

		case "cluster-state":
			c.GetClusterState()

		default:
			fmt.Printf("❌ Unknown command: %s (type 'help' for commands)\n", cmd)
		}
	}
}

func showHelp() {
	fmt.Println("\n📖 Command Help:")
	fmt.Println("=")
	fmt.Println("create-user <name>")
	fmt.Println("  Create a new user")
	fmt.Println("  Example: create-user Alice")
	fmt.Println()
	fmt.Println("create-topic <name>")
	fmt.Println("  Create a new topic")
	fmt.Println("  Example: create-topic Golang")
	fmt.Println()
	fmt.Println("post <topic_id> <user_id> <text>")
	fmt.Println("  Post a message to a topic")
	fmt.Println("  Example: post 1 1 Hello world!")
	fmt.Println()
	fmt.Println("like <topic_id> <message_id> <user_id>")
	fmt.Println("  Like a message")
	fmt.Println("  Example: like 1 1 2")
	fmt.Println()
	fmt.Println("list-topics")
	fmt.Println("  List all topics")
	fmt.Println()
	fmt.Println("get-messages <topic_id> [from_id] [limit]")
	fmt.Println("  Get messages from a topic")
	fmt.Println("  Example: get-messages 1")
	fmt.Println("  Example: get-messages 1 0 10")
	fmt.Println()
	fmt.Println("subscribe <user_id> <topic_id1,topic_id2,...> [from_id]")
	fmt.Println("  Subscribe to topics (real-time)")
	fmt.Println("  Example: subscribe 1 1,2")
	fmt.Println("  Example: subscribe 1 1 0")
	fmt.Println()
	fmt.Println("cluster-state")
	fmt.Println("  Show cluster state")
	fmt.Println()
	fmt.Println("=")
}
