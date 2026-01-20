package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "razpravljalnica/proto"
)

// Client za komunikacijo s serverjem
type Client struct {
	conn     *grpc.ClientConn
	client   pb.MessageBoardClient
	cpClient pb.ControlPlaneClient
}

// NewClient ustvari novega klienta
func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	return &Client{
		conn:     conn,
		client:   pb.NewMessageBoardClient(conn),
		cpClient: pb.NewControlPlaneClient(conn),
	}, nil
}

// Close zapre povezavo
func (c *Client) Close() {
	c.conn.Close()
}

// CreateUser ustvari novega uporabnika
func (c *Client) CreateUser(name string) (*pb.User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user, err := c.client.CreateUser(ctx, &pb.CreateUserRequest{Name: name})
	if err != nil {
		return nil, err
	}

	fmt.Printf("✅ User created: %s (ID: %d)\n", user.Name, user.Id)
	return user, nil
}

// CreateTopic ustvari novo temo
func (c *Client) CreateTopic(name string) (*pb.Topic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic, err := c.client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
	if err != nil {
		return nil, err
	}

	fmt.Printf("✅ Topic created: %s (ID: %d)\n", topic.Name, topic.Id)
	return topic, nil
}

// PostMessage objavi sporočilo
func (c *Client) PostMessage(topicID, userID int64, text string) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.client.PostMessage(ctx, &pb.PostMessageRequest{
		TopicId: topicID,
		UserId:  userID,
		Text:    text,
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("✅ Message posted (ID: %d): %s\n", msg.Id, msg.Text)
	return msg, nil
}

// LikeMessage všečka sporočilo
func (c *Client) LikeMessage(topicID, messageID, userID int64) (*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.client.LikeMessage(ctx, &pb.LikeMessageRequest{
		TopicId:   topicID,
		MessageId: messageID,
		UserId:    userID,
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("✅ Message liked! Total likes: %d\n", msg.Likes)
	return msg, nil
}

// ListTopics izpiše vse teme
func (c *Client) ListTopics() ([]*pb.Topic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	fmt.Println("\n📋 Topics:")
	fmt.Println("=")
	for _, topic := range resp.Topics {
		fmt.Printf("  [%d] %s\n", topic.Id, topic.Name)
	}
	fmt.Println("=")

	return resp.Topics, nil
}

// GetMessages pridobi sporočila iz teme
func (c *Client) GetMessages(topicID, fromMessageID int64, limit int32) ([]*pb.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.GetMessages(ctx, &pb.GetMessagesRequest{
		TopicId:       topicID,
		FromMessageId: fromMessageID,
		Limit:         limit,
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("\n💬 Messages in topic %d:\n", topicID)
	fmt.Println("=")
	for _, msg := range resp.Messages {
		timestamp := msg.CreatedAt.AsTime().Format("15:04:05")
		fmt.Printf("[%d] User %d at %s (❤️  %d):\n  %s\n\n",
			msg.Id, msg.UserId, timestamp, msg.Likes, msg.Text)
	}
	fmt.Println("=")

	return resp.Messages, nil
}

// GetSubscriptionNode pridobi node za subscription
func (c *Client) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	return c.client.GetSubscriptionNode(ctx, req)
}

// SubscribeToTopic naroči se na temo
func (c *Client) SubscribeToTopic(ctx context.Context, req *pb.SubscribeTopicRequest) (pb.MessageBoard_SubscribeTopicClient, error) {
	return c.client.SubscribeTopic(ctx, req)
}

// SubscribeTopic naroči na teme (blocking)
func (c *Client) SubscribeTopic(userID int64, topicIDs []int64, fromMessageID int64) error {
	// Najprej pridobi subscription node
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	subNodeResp, err := c.client.GetSubscriptionNode(ctx1, &pb.SubscriptionNodeRequest{
		UserId:  userID,
		TopicId: topicIDs,
	})
	if err != nil {
		return fmt.Errorf("failed to get subscription node: %v", err)
	}

	fmt.Printf("📡 Subscribing to topics %v (token: %s)\n", topicIDs, subNodeResp.SubscribeToken)

	// Odpri stream za subscription
	ctx2 := context.Background()
	stream, err := c.client.SubscribeTopic(ctx2, &pb.SubscribeTopicRequest{
		TopicId:        topicIDs,
		UserId:         userID,
		FromMessageId:  fromMessageID,
		SubscribeToken: subNodeResp.SubscribeToken,
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	fmt.Println("🔔 Listening for messages... (Press Ctrl+C to stop)")
	fmt.Println("=")

	// Preberi events
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("stream error: %v", err)
		}

		// Prikaži event
		c.displayEvent(event)
	}

	return nil
}

// displayEvent prikaže event na konzoli
func (c *Client) displayEvent(event *pb.MessageEvent) {
	timestamp := event.EventAt.AsTime().Format("15:04:05")
	msg := event.Message

	switch event.Op {
	case pb.OpType_OP_POST:
		fmt.Printf("📨 [%s] NEW MESSAGE in topic %d:\n", timestamp, msg.TopicId)
		fmt.Printf("   User %d: %s\n", msg.UserId, msg.Text)
	case pb.OpType_OP_LIKE:
		fmt.Printf("❤️  [%s] Message %d liked! Total: %d\n", timestamp, msg.Id, msg.Likes)
	}
	fmt.Println()
}

// GetClusterState pridobi stanje cluster-ja
func (c *Client) GetClusterState() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.cpClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	fmt.Println("\n🌐 Cluster State:")
	fmt.Println("=")
	fmt.Printf("HEAD: %s (%s)\n", resp.Head.NodeId, resp.Head.Address)
	fmt.Printf("TAIL: %s (%s)\n", resp.Tail.NodeId, resp.Tail.Address)
	fmt.Println("=")

	return nil
}
