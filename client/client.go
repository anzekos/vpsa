package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client je odjemalec razpravljalnice
type Client struct {
	masterAddr    string
	masterClient  pb.ControlPlaneClient
	headClient    pb.MessageBoardClient
	tailClient    pb.MessageBoardClient
	currentUserID int64
}

// NewClient ustvari novega odjemalca
func NewClient(masterAddr string) (*Client, error) {
	// Connect to master
	conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %v", err)
	}

	masterClient := pb.NewControlPlaneClient(conn)

	client := &Client{
		masterAddr:   masterAddr,
		masterClient: masterClient,
	}

	// Get cluster state and connect to head/tail
	if err := client.refreshConnections(); err != nil {
		return nil, err
	}

	return client, nil
}

// refreshConnections posodobi povezave na glavo in rep
func (c *Client) refreshConnections() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := c.masterClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to get cluster state: %v", err)
	}

	// Connect to head
	headConn, err := grpc.Dial(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to head: %v", err)
	}
	c.headClient = pb.NewMessageBoardClient(headConn)

	// Connect to tail
	tailConn, err := grpc.Dial(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to tail: %v", err)
	}
	c.tailClient = pb.NewMessageBoardClient(tailConn)

	log.Printf("Connected - Head: %s, Tail: %s", state.Head.NodeId, state.Tail.NodeId)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Write operations (go to head)
////////////////////////////////////////////////////////////////////////////////

// CreateUser ustvari novega uporabnika
func (c *Client) CreateUser(name string) (*pb.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.CreateUserRequest{Name: name}
	user, err := c.headClient.CreateUser(ctx, req)
	if err != nil {
		// Try refreshing connections
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			user, err = c.headClient.CreateUser(ctx, req)
		}
	}

	if err == nil {
		c.currentUserID = user.Id
		log.Printf("Created user: %s (ID: %d)", user.Name, user.Id)
	}

	return user, err
}

// CreateTopic ustvari novo temo
func (c *Client) CreateTopic(name string) (*pb.Topic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.CreateTopicRequest{Name: name}
	topic, err := c.headClient.CreateTopic(ctx, req)
	if err != nil {
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			topic, err = c.headClient.CreateTopic(ctx, req)
		}
	}

	if err == nil {
		log.Printf("Created topic: %s (ID: %d)", topic.Name, topic.Id)
	}

	return topic, err
}

// PostMessage objavi sporočilo
func (c *Client) PostMessage(topicID int64, text string) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  c.currentUserID,
		Text:    text,
	}

	msg, err := c.headClient.PostMessage(ctx, req)
	if err != nil {
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			msg, err = c.headClient.PostMessage(ctx, req)
		}
	}

	if err == nil {
		log.Printf("Posted message %d to topic %d", msg.Id, topicID)
	}

	return msg, err
}

// LikeMessage všečka sporočilo
func (c *Client) LikeMessage(topicID, messageID int64) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.LikeMessageRequest{
		TopicId:   topicID,
		MessageId: messageID,
		UserId:    c.currentUserID,
	}

	msg, err := c.headClient.LikeMessage(ctx, req)
	if err != nil {
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			msg, err = c.headClient.LikeMessage(ctx, req)
		}
	}

	if err == nil {
		log.Printf("Liked message %d (now has %d likes)", messageID, msg.Likes)
	}

	return msg, err
}

////////////////////////////////////////////////////////////////////////////////
// Read operations (go to tail)
////////////////////////////////////////////////////////////////////////////////

// ListTopics vrne vse teme
func (c *Client) ListTopics() ([]*pb.Topic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.tailClient.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			resp, err = c.tailClient.ListTopics(ctx, &emptypb.Empty{})
		}
	}

	if err != nil {
		return nil, err
	}

	return resp.Topics, nil
}

// GetMessages vrne sporočila iz teme
func (c *Client) GetMessages(topicID, fromMessageID int64, limit int32) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.GetMessagesRequest{
		TopicId:       topicID,
		FromMessageId: fromMessageID,
		Limit:         limit,
	}

	resp, err := c.tailClient.GetMessages(ctx, req)
	if err != nil {
		if refreshErr := c.refreshConnections(); refreshErr == nil {
			resp, err = c.tailClient.GetMessages(ctx, req)
		}
	}

	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

////////////////////////////////////////////////////////////////////////////////
// Subscription
////////////////////////////////////////////////////////////////////////////////

// Subscribe se naroči na teme
func (c *Client) Subscribe(topicIDs []int64, fromMessageID int64, handler func(*pb.MessageEvent)) error {
	ctx := context.Background()

	// Get subscription node from head
	subReq := &pb.SubscriptionNodeRequest{
		UserId:  c.currentUserID,
		TopicId: topicIDs,
	}

	subResp, err := c.headClient.GetSubscriptionNode(ctx, subReq)
	if err != nil {
		return fmt.Errorf("failed to get subscription node: %v", err)
	}

	// Connect to subscription node
	conn, err := grpc.Dial(subResp.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to subscription node: %v", err)
	}
	defer conn.Close()

	subClient := pb.NewMessageBoardClient(conn)

	// Subscribe
	subTopicReq := &pb.SubscribeTopicRequest{
		TopicId:         topicIDs,
		UserId:          c.currentUserID,
		FromMessageId:   fromMessageID,
		SubscribeToken:  subResp.SubscribeToken,
	}

	stream, err := subClient.SubscribeTopic(ctx, subTopicReq)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	log.Printf("Subscribed to topics: %v", topicIDs)

	// Receive events
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			log.Println("Subscription stream ended")
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving event: %v", err)
		}

		handler(event)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Interactive CLI
////////////////////////////////////////////////////////////////////////////////

// RunInteractive zažene interaktivni CLI
func (c *Client) RunInteractive() {
	fmt.Println("\n=== Razpravljalnica Client ===")
	fmt.Println("Commands:")
	fmt.Println("  1 - Create user")
	fmt.Println("  2 - Create topic")
	fmt.Println("  3 - List topics")
	fmt.Println("  4 - Post message")
	fmt.Println("  5 - View messages")
	fmt.Println("  6 - Like message")
	fmt.Println("  7 - Subscribe to topic")
	fmt.Println("  0 - Exit")
	fmt.Println()

	for {
		fmt.Print("Enter command: ")
		var cmd int
		fmt.Scanln(&cmd)

		switch cmd {
		case 0:
			return

		case 1:
			fmt.Print("Enter username: ")
			var name string
			fmt.Scanln(&name)
			user, err := c.CreateUser(name)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Created user: %s (ID: %d)\n", user.Name, user.Id)
			}

		case 2:
			fmt.Print("Enter topic name: ")
			var name string
			fmt.Scanln(&name)
			topic, err := c.CreateTopic(name)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Created topic: %s (ID: %d)\n", topic.Name, topic.Id)
			}

		case 3:
			topics, err := c.ListTopics()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("\nTopics:")
				for _, t := range topics {
					fmt.Printf("  [%d] %s\n", t.Id, t.Name)
				}
			}

		case 4:
			fmt.Print("Enter topic ID: ")
			var topicID int64
			fmt.Scanln(&topicID)
			fmt.Print("Enter message: ")
			var text string
			fmt.Scanln(&text)
			msg, err := c.PostMessage(topicID, text)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Posted message ID: %d\n", msg.Id)
			}

		case 5:
			fmt.Print("Enter topic ID: ")
			var topicID int64
			fmt.Scanln(&topicID)
			messages, err := c.GetMessages(topicID, 0, 50)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("\nMessages in topic %d:\n", topicID)
				for _, m := range messages {
					fmt.Printf("  [%d] User %d: %s (likes: %d)\n",
						m.Id, m.UserId, m.Text, m.Likes)
				}
			}

		case 6:
			fmt.Print("Enter topic ID: ")
			var topicID int64
			fmt.Scanln(&topicID)
			fmt.Print("Enter message ID: ")
			var msgID int64
			fmt.Scanln(&msgID)
			msg, err := c.LikeMessage(topicID, msgID)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Message now has %d likes\n", msg.Likes)
			}

		case 7:
			fmt.Print("Enter topic IDs (comma separated): ")
			var input string
			fmt.Scanln(&input)
			// Simplified - in real impl, parse comma-separated IDs
			var topicID int64
			fmt.Sscanf(input, "%d", &topicID)
			topicIDs := []int64{topicID}

			fmt.Println("Starting subscription (Ctrl+C to stop)...")
			err := c.Subscribe(topicIDs, 0, func(event *pb.MessageEvent) {
				fmt.Printf("\n[EVENT] Topic %d, Message %d: %s (likes: %d)\n",
					event.Message.TopicId, event.Message.Id,
					event.Message.Text, event.Message.Likes)
			})
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		default:
			fmt.Println("Unknown command")
		}
	}
}
