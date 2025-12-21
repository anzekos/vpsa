package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"razpravljalnica/pb"
	"razpravljalnica/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Node je vozlišče v verižni replikaciji
type Node struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedChainReplicationServer

	nodeID   string
	address  string
	storage  *storage.Storage
	grpcServer *grpc.Server

	// Chain topology
	mu          sync.RWMutex
	isHead      bool
	isTail      bool
	predecessor *pb.NodeInfo
	successor   *pb.NodeInfo

	// Pending updates (for Sent_i in paper)
	pendingMu      sync.Mutex
	pendingUpdates map[int64]*pb.UpdateRequest // seq_num -> update

	// Subscription management
	subscriptions sync.Map // token -> subscription channels

	// gRPC clients
	successorClient   pb.ChainReplicationClient
	predecessorClient pb.ChainReplicationClient
}

func NewNode(address string) *Node {
	// Generate unique node ID
	b := make([]byte, 8)
	rand.Read(b)
	nodeID := hex.EncodeToString(b)

	return &Node{
		nodeID:         nodeID,
		address:        address,
		storage:        storage.NewStorage(),
		isHead:         false,
		isTail:         false,
		pendingUpdates: make(map[int64]*pb.UpdateRequest),
	}
}

// Start pokrene gRPC strežnik
func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	n.grpcServer = grpc.NewServer()
	pb.RegisterMessageBoardServer(n.grpcServer, n)
	pb.RegisterChainReplicationServer(n.grpcServer, n)

	log.Printf("Node %s starting on %s", n.nodeID, n.address)

	return n.grpcServer.Serve(lis)
}

// Stop ustavi vozlišče
func (n *Node) Stop() {
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}
}

////////////////////////////////////////////////////////////////////////////////
// MessageBoard API - Client facing operations
////////////////////////////////////////////////////////////////////////////////

// CreateUser - gre na glavo
func (n *Node) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	n.mu.RLock()
	isHead := n.isHead
	n.mu.RUnlock()

	if !isHead {
		return nil, fmt.Errorf("not head node, cannot process write")
	}

	// Create locally
	user, err := n.storage.CreateUser(req.Name)
	if err != nil {
		return nil, err
	}

	// Forward to successor
	update := &pb.UpdateRequest{
		SequenceNumber: n.storage.GetSequenceNumber(),
		OpType:         pb.OpType_OP_POST,
		Operation: &pb.UpdateRequest_CreateUser{
			CreateUser: req,
		},
	}

	n.forwardUpdate(update)

	return &pb.User{
		Id:   user.ID,
		Name: user.Name,
	}, nil
}

// CreateTopic - gre na glavo
func (n *Node) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	n.mu.RLock()
	isHead := n.isHead
	n.mu.RUnlock()

	if !isHead {
		return nil, fmt.Errorf("not head node, cannot process write")
	}

	topic, err := n.storage.CreateTopic(req.Name)
	if err != nil {
		return nil, err
	}

	update := &pb.UpdateRequest{
		SequenceNumber: n.storage.GetSequenceNumber(),
		OpType:         pb.OpType_OP_POST,
		Operation: &pb.UpdateRequest_CreateTopic{
			CreateTopic: req,
		},
	}

	n.forwardUpdate(update)

	return &pb.Topic{
		Id:   topic.ID,
		Name: topic.Name,
	}, nil
}

// PostMessage - gre na glavo
func (n *Node) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	n.mu.RLock()
	isHead := n.isHead
	n.mu.RUnlock()

	if !isHead {
		return nil, fmt.Errorf("not head node, cannot process write")
	}

	msg, err := n.storage.PostMessage(req.TopicId, req.UserId, req.Text)
	if err != nil {
		return nil, err
	}

	update := &pb.UpdateRequest{
		SequenceNumber: n.storage.GetSequenceNumber(),
		OpType:         pb.OpType_OP_POST,
		Operation: &pb.UpdateRequest_PostMessage{
			PostMessage: req,
		},
	}

	n.forwardUpdate(update)

	// Notify subscribers
	n.notifySubscribers(msg)

	return &pb.Message{
		Id:        msg.ID,
		TopicId:   msg.TopicID,
		UserId:    msg.UserID,
		Text:      msg.Text,
		CreatedAt: timestamppb.New(msg.CreatedAt),
		Likes:     msg.Likes,
	}, nil
}

// LikeMessage - gre na glavo
func (n *Node) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	n.mu.RLock()
	isHead := n.isHead
	n.mu.RUnlock()

	if !isHead {
		return nil, fmt.Errorf("not head node, cannot process write")
	}

	msg, err := n.storage.LikeMessage(req.TopicId, req.MessageId, req.UserId)
	if err != nil {
		return nil, err
	}

	update := &pb.UpdateRequest{
		SequenceNumber: n.storage.GetSequenceNumber(),
		OpType:         pb.OpType_OP_LIKE,
		Operation: &pb.UpdateRequest_LikeMessage{
			LikeMessage: req,
		},
	}

	n.forwardUpdate(update)

	// Notify subscribers
	n.notifySubscribers(msg)

	return &pb.Message{
		Id:        msg.ID,
		TopicId:   msg.TopicID,
		UserId:    msg.UserID,
		Text:      msg.Text,
		CreatedAt: timestamppb.New(msg.CreatedAt),
		Likes:     msg.Likes,
	}, nil
}

// ListTopics - gre na rep
func (n *Node) ListTopics(ctx context.Context, _ *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	n.mu.RLock()
	isTail := n.isTail
	n.mu.RUnlock()

	if !isTail {
		return nil, fmt.Errorf("not tail node, cannot process read")
	}

	topics := n.storage.ListTopics()
	pbTopics := make([]*pb.Topic, len(topics))
	for i, t := range topics {
		pbTopics[i] = &pb.Topic{
			Id:   t.ID,
			Name: t.Name,
		}
	}

	return &pb.ListTopicsResponse{Topics: pbTopics}, nil
}

// GetMessages - gre na rep
func (n *Node) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	n.mu.RLock()
	isTail := n.isTail
	n.mu.RUnlock()

	if !isTail {
		return nil, fmt.Errorf("not tail node, cannot process read")
	}

	messages, err := n.storage.GetMessages(req.TopicId, req.FromMessageId, req.Limit)
	if err != nil {
		return nil, err
	}

	pbMessages := make([]*pb.Message, len(messages))
	for i, m := range messages {
		pbMessages[i] = &pb.Message{
			Id:        m.ID,
			TopicId:   m.TopicID,
			UserId:    m.UserID,
			Text:      m.Text,
			CreatedAt: timestamppb.New(m.CreatedAt),
			Likes:     m.Likes,
		}
	}

	return &pb.GetMessagesResponse{Messages: pbMessages}, nil
}

// GetSubscriptionNode - glava dodelj vozlišče za subscription
func (n *Node) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	n.mu.RLock()
	isHead := n.isHead
	n.mu.RUnlock()

	if !isHead {
		return nil, fmt.Errorf("not head node")
	}

	// Generate subscription token
	token := n.generateToken()

	// Za load balancing - v tej implementaciji vrnemo kar sebe
	// V produkciji bi lahko uporabljali round-robin ali drug algoritem
	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node: &pb.NodeInfo{
			NodeId:  n.nodeID,
			Address: n.address,
		},
	}, nil
}

// SubscribeTopic - streaming subscriptions
func (n *Node) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	// Create channel for this subscription
	msgChan := make(chan *pb.MessageEvent, 100)
	
	// Register subscription
	n.subscriptions.Store(req.SubscribeToken, msgChan)
	defer n.subscriptions.Delete(req.SubscribeToken)

	// Send existing messages first
	for _, topicID := range req.TopicId {
		messages, err := n.storage.GetMessages(topicID, req.FromMessageId, 0)
		if err != nil {
			continue
		}

		for _, msg := range messages {
			event := &pb.MessageEvent{
				SequenceNumber: msg.ID,
				Op:             pb.OpType_OP_POST,
				Message: &pb.Message{
					Id:        msg.ID,
					TopicId:   msg.TopicID,
					UserId:    msg.UserID,
					Text:      msg.Text,
					CreatedAt: timestamppb.New(msg.CreatedAt),
					Likes:     msg.Likes,
				},
				EventAt: timestamppb.New(msg.CreatedAt),
			}

			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}

	// Stream new messages
	for {
		select {
		case event := <-msgChan:
			// Check if message is for subscribed topics
			subscribed := false
			for _, topicID := range req.TopicId {
				if event.Message.TopicId == topicID {
					subscribed = true
					break
				}
			}

			if subscribed {
				if err := stream.Send(event); err != nil {
					return err
				}
			}

		case <-stream.Context().Done():
			return nil
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Chain Replication Protocol
////////////////////////////////////////////////////////////////////////////////

// ForwardUpdate sprejme update od predhodnika
func (n *Node) ForwardUpdate(ctx context.Context, req *pb.UpdateRequest) (*emptypb.Empty, error) {
	// Apply update locally
	n.applyUpdate(req)

	// Forward to successor (if not tail)
	n.mu.RLock()
	isTail := n.isTail
	n.mu.RUnlock()

	if !isTail {
		n.forwardUpdate(req)
	} else {
		// If tail, send ACK back
		n.sendAck(req.SequenceNumber)
	}

	return &emptypb.Empty{}, nil
}

// AckUpdate sprejme ACK od naslednika
func (n *Node) AckUpdate(ctx context.Context, req *pb.AckRequest) (*emptypb.Empty, error) {
	n.pendingMu.Lock()
	delete(n.pendingUpdates, req.SequenceNumber)
	n.pendingMu.Unlock()

	// Forward ACK to predecessor
	n.mu.RLock()
	isHead := n.isHead
	pred := n.predecessor
	n.mu.RUnlock()

	if !isHead && pred != nil {
		client, err := n.getOrCreatePredecessorClient()
		if err == nil {
			client.AckUpdate(context.Background(), req)
		}
	}

	return &emptypb.Empty{}, nil
}

// SyncState vrne snapshot stanja (za recovery)
func (n *Node) SyncState(ctx context.Context, _ *emptypb.Empty) (*pb.StateSnapshot, error) {
	snapshot := n.storage.GetSnapshot()

	pbSnapshot := &pb.StateSnapshot{
		Users:          make(map[int64]*pb.User),
		Topics:         make(map[int64]*pb.Topic),
		Messages:       make(map[int64]*pb.MessageList),
		LastUserId:     snapshot.LastUserID,
		LastTopicId:    snapshot.LastTopicID,
		LastMessageId:  snapshot.LastMessageID,
		SequenceNumber: snapshot.SequenceNumber,
	}

	for id, user := range snapshot.Users {
		pbSnapshot.Users[id] = &pb.User{Id: user.ID, Name: user.Name}
	}

	for id, topic := range snapshot.Topics {
		pbSnapshot.Topics[id] = &pb.Topic{Id: topic.ID, Name: topic.Name}
	}

	for topicID, messages := range snapshot.Messages {
		pbMessages := make([]*pb.Message, 0, len(messages))
		for _, msg := range messages {
			pbMessages = append(pbMessages, &pb.Message{
				Id:        msg.ID,
				TopicId:   msg.TopicID,
				UserId:    msg.UserID,
				Text:      msg.Text,
				CreatedAt: timestamppb.New(msg.CreatedAt),
				Likes:     msg.Likes,
			})
		}
		pbSnapshot.Messages[topicID] = &pb.MessageList{Messages: pbMessages}
	}

	return pbSnapshot, nil
}

// Heartbeat za failure detection
func (n *Node) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// UpdateTopology posodobi topologijo verige
func (n *Node) UpdateTopology(ctx context.Context, req *pb.TopologyUpdate) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("Node %s: topology update - head=%v, tail=%v", n.nodeID, req.IsHead, req.IsTail)

	n.isHead = req.IsHead
	n.isTail = req.IsTail
	n.predecessor = req.Predecessor
	n.successor = req.Successor

	// Re-establish connections
	n.successorClient = nil
	n.predecessorClient = nil

	// Re-send pending updates if needed
	if len(req.PendingAcks) > 0 {
		go n.resendPendingUpdates(req.PendingAcks)
	}

	return &emptypb.Empty{}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Helper methods
////////////////////////////////////////////////////////////////////////////////

func (n *Node) forwardUpdate(update *pb.UpdateRequest) {
	n.mu.RLock()
	succ := n.successor
	n.mu.RUnlock()

	if succ == nil {
		return
	}

	// Add to pending
	n.pendingMu.Lock()
	n.pendingUpdates[update.SequenceNumber] = update
	n.pendingMu.Unlock()

	// Forward to successor
	client, err := n.getOrCreateSuccessorClient()
	if err != nil {
		log.Printf("Failed to connect to successor: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.ForwardUpdate(ctx, update)
	if err != nil {
		log.Printf("Failed to forward update: %v", err)
	}
}

func (n *Node) applyUpdate(update *pb.UpdateRequest) {
	switch op := update.Operation.(type) {
	case *pb.UpdateRequest_CreateUser:
		n.storage.CreateUser(op.CreateUser.Name)
	case *pb.UpdateRequest_CreateTopic:
		n.storage.CreateTopic(op.CreateTopic.Name)
	case *pb.UpdateRequest_PostMessage:
		msg, _ := n.storage.PostMessage(op.PostMessage.TopicId, op.PostMessage.UserId, op.PostMessage.Text)
		if msg != nil {
			n.notifySubscribers(msg)
		}
	case *pb.UpdateRequest_LikeMessage:
		msg, _ := n.storage.LikeMessage(op.LikeMessage.TopicId, op.LikeMessage.MessageId, op.LikeMessage.UserId)
		if msg != nil {
			n.notifySubscribers(msg)
		}
	}
}

func (n *Node) sendAck(seqNum int64) {
	n.mu.RLock()
	pred := n.predecessor
	n.mu.RUnlock()

	if pred == nil {
		return
	}

	client, err := n.getOrCreatePredecessorClient()
	if err != nil {
		return
	}

	client.AckUpdate(context.Background(), &pb.AckRequest{SequenceNumber: seqNum})
}

func (n *Node) getOrCreateSuccessorClient() (pb.ChainReplicationClient, error) {
	if n.successorClient != nil {
		return n.successorClient, nil
	}

	n.mu.RLock()
	succ := n.successor
	n.mu.RUnlock()

	if succ == nil {
		return nil, fmt.Errorf("no successor")
	}

	conn, err := grpc.Dial(succ.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	n.successorClient = pb.NewChainReplicationClient(conn)
	return n.successorClient, nil
}

func (n *Node) getOrCreatePredecessorClient() (pb.ChainReplicationClient, error) {
	if n.predecessorClient != nil {
		return n.predecessorClient, nil
	}

	n.mu.RLock()
	pred := n.predecessor
	n.mu.RUnlock()

	if pred == nil {
		return nil, fmt.Errorf("no predecessor")
	}

	conn, err := grpc.Dial(pred.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	n.predecessorClient = pb.NewChainReplicationClient(conn)
	return n.predecessorClient, nil
}

func (n *Node) resendPendingUpdates(seqNums []int64) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	for _, seqNum := range seqNums {
		if update, exists := n.pendingUpdates[seqNum]; exists {
			go n.forwardUpdate(update)
		}
	}
}

func (n *Node) notifySubscribers(msg *storage.Message) {
	event := &pb.MessageEvent{
		SequenceNumber: msg.ID,
		Op:             pb.OpType_OP_POST,
		Message: &pb.Message{
			Id:        msg.ID,
			TopicId:   msg.TopicID,
			UserId:    msg.UserID,
			Text:      msg.Text,
			CreatedAt: timestamppb.New(msg.CreatedAt),
			Likes:     msg.Likes,
		},
		EventAt: timestamppb.New(time.Now()),
	}

	n.subscriptions.Range(func(key, value interface{}) bool {
		ch := value.(chan *pb.MessageEvent)
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
		return true
	})
}

func (n *Node) generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
