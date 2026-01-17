package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "razpravljalnica/proto"
)

// NodeRole določa vlogo vozlišča v verigi
type NodeRole string

const (
	RoleHead   NodeRole = "HEAD"
	RoleMiddle NodeRole = "MIDDLE"
	RoleTail   NodeRole = "TAIL"
)

// ChainNode predstavlja vozlišče v verigi
type ChainNode struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedReplicationServiceServer
	pb.UnimplementedControlPlaneServer

	// Node info
	nodeID  string
	address string
	role    NodeRole

	// Storage (enak kot v osnovni verziji)
	mu              sync.RWMutex
	users           map[int64]*pb.User
	topics          map[int64]*pb.Topic
	messages        map[int64]*pb.Message
	messagesByTopic map[int64][]*pb.Message

	// Subscriptions
	subscriptions     map[int64]*Subscription
	subscriptionMutex sync.RWMutex
	nextSubNodeIdx    int64 // Za round-robin load balancing

	// Chain topology
	successor       *pb.NodeInfo
	successorConn   *grpc.ClientConn
	successorClient pb.ReplicationServiceClient

	predecessor *pb.NodeInfo

	chainNodes []*pb.NodeInfo // Vsa vozlišča v verigi (za subscription routing)
	chainMutex sync.RWMutex

	// Control plane connection
	controlPlaneAddr   string
	controlPlaneConn   *grpc.ClientConn
	controlPlaneClient pb.NodeManagementClient

	// Heartbeat
	heartbeatStop chan struct{}

	// Sequence counter
	nextSeqNumber int64
}

// NewChainNode ustvari novo vozlišče v verigi
func NewChainNode(nodeID, address string, role NodeRole, controlPlaneAddr string) *ChainNode {
	return &ChainNode{
		nodeID:           nodeID,
		address:          address,
		role:             role,
		users:            make(map[int64]*pb.User),
		topics:           make(map[int64]*pb.Topic),
		messages:         make(map[int64]*pb.Message),
		messagesByTopic:  make(map[int64][]*pb.Message),
		subscriptions:    make(map[int64]*Subscription),
		chainNodes:       make([]*pb.NodeInfo, 0),
		controlPlaneAddr: controlPlaneAddr,
		heartbeatStop:    make(chan struct{}),
		nextSeqNumber:    0,
		nextSubNodeIdx:   0,
	}
}

// SetSuccessor nastavi naslednika v verigi
func (n *ChainNode) SetSuccessor(successorAddr string) error {
	if successorAddr == "" {
		n.successor = nil
		return nil
	}

	// Poveži se na naslednika
	conn, err := grpc.Dial(successorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect to successor: %v", err)
	}

	n.successorConn = conn
	n.successorClient = pb.NewReplicationServiceClient(conn)

	// Ping za preverjanje
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := n.successorClient.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("failed to ping successor: %v", err)
	}

	n.successor = &pb.NodeInfo{
		NodeId:  resp.NodeId,
		Address: successorAddr,
	}

	log.Printf("[%s] Successor set to: %s (%s)", n.nodeID, resp.NodeId, successorAddr)
	return nil
}

// RegisterChainNode registrira vozlišče za subscription routing
func (n *ChainNode) RegisterChainNode(nodeInfo *pb.NodeInfo) {
	n.chainMutex.Lock()
	defer n.chainMutex.Unlock()

	// Preveri če že obstaja
	for _, existing := range n.chainNodes {
		if existing.NodeId == nodeInfo.NodeId {
			return
		}
	}

	n.chainNodes = append(n.chainNodes, nodeInfo)
	log.Printf("[%s] Registered chain node: %s", n.nodeID, nodeInfo.NodeId)
}

// ============================================================================
// DATA PLANE - Client-facing methods
// ============================================================================

// CreateUser - samo HEAD sprejema (ali če predecessor ni nastavljen)
func (n *ChainNode) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	// Preveri če smo HEAD (nimamo predhodnika)
	if n.predecessor != nil {
		return nil, status.Error(codes.FailedPrecondition, "only HEAD can create users")
	}

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name cannot be empty")
	}

	n.mu.Lock()
	id := int64(len(n.users) + 1)
	user := &pb.User{Id: id, Name: req.Name}
	n.users[id] = user
	n.mu.Unlock()

	log.Printf("[%s] Created user: %s (ID: %d)", n.nodeID, user.Name, user.Id)

	// Repliciraj naprej
	if n.successor != nil {
		go n.replicateUser(user)
	}

	return user, nil
}

// CreateTopic - samo HEAD sprejema
func (n *ChainNode) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if n.predecessor != nil {
		return nil, status.Error(codes.FailedPrecondition, "only HEAD can create topics")
	}

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name cannot be empty")
	}

	n.mu.Lock()
	id := int64(len(n.topics) + 1)
	topic := &pb.Topic{Id: id, Name: req.Name}
	n.topics[id] = topic
	n.messagesByTopic[id] = make([]*pb.Message, 0)
	n.mu.Unlock()

	log.Printf("[%s] Created topic: %s (ID: %d)", n.nodeID, topic.Name, topic.Id)

	// Repliciraj naprej
	if n.successor != nil {
		go n.replicateTopic(topic)
	}

	return topic, nil
}

// PostMessage - samo HEAD sprejema
func (n *ChainNode) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if n.predecessor != nil {
		return nil, status.Error(codes.FailedPrecondition, "only HEAD can post messages")
	}

	n.mu.Lock()

	// Validacija
	if _, exists := n.topics[req.TopicId]; !exists {
		n.mu.Unlock()
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	if _, exists := n.users[req.UserId]; !exists {
		n.mu.Unlock()
		return nil, status.Error(codes.NotFound, "user not found")
	}

	// Ustvari message
	id := int64(len(n.messages) + 1)
	message := &pb.Message{
		Id:        id,
		TopicId:   req.TopicId,
		UserId:    req.UserId,
		Text:      req.Text,
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}

	n.messages[id] = message
	n.messagesByTopic[req.TopicId] = append(n.messagesByTopic[req.TopicId], message)
	n.mu.Unlock()

	log.Printf("[%s] Posted message %d to topic %d", n.nodeID, id, req.TopicId)

	// Repliciraj naprej
	if n.successor != nil {
		go n.replicateMessage(message)
	}

	// Obvesti lokalne naročnike
	go n.notifySubscribers(req.TopicId, pb.OpType_OP_POST, message)

	return message, nil
}

// LikeMessage - samo HEAD sprejema
func (n *ChainNode) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	if n.predecessor != nil {
		return nil, status.Error(codes.FailedPrecondition, "only HEAD can like messages")
	}

	n.mu.Lock()
	message, exists := n.messages[req.MessageId]
	if !exists {
		n.mu.Unlock()
		return nil, status.Error(codes.NotFound, "message not found")
	}

	message.Likes++
	newLikes := message.Likes
	n.mu.Unlock()

	log.Printf("[%s] Message %d liked (total: %d)", n.nodeID, req.MessageId, newLikes)

	// Repliciraj naprej
	if n.successor != nil {
		go n.replicateLike(req.MessageId, req.UserId, newLikes)
	}

	// Obvesti lokalne naročnike
	go n.notifySubscribers(message.TopicId, pb.OpType_OP_LIKE, message)

	return message, nil
}

// GetSubscriptionNode - samo HEAD določa kam gre subscription
func (n *ChainNode) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	if n.predecessor != nil {
		return nil, status.Error(codes.FailedPrecondition, "only HEAD assigns subscriptions")
	}

	n.chainMutex.RLock()
	nodeCount := len(n.chainNodes)
	n.chainMutex.RUnlock()

	if nodeCount == 0 {
		// Ni drugih nodes, uporabi ta node
		token := fmt.Sprintf("token-%d-%d", req.UserId, time.Now().UnixNano())
		return &pb.SubscriptionNodeResponse{
			SubscribeToken: token,
			Node:           &pb.NodeInfo{NodeId: n.nodeID, Address: n.address},
		}, nil
	}

	// Round-robin load balancing
	idx := atomic.AddInt64(&n.nextSubNodeIdx, 1) % int64(nodeCount)

	n.chainMutex.RLock()
	selectedNode := n.chainNodes[idx]
	n.chainMutex.RUnlock()

	token := fmt.Sprintf("token-%d-%d", req.UserId, time.Now().UnixNano())

	log.Printf("[%s] Assigned subscription for user %d to node %s",
		n.nodeID, req.UserId, selectedNode.NodeId)

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node:           selectedNode,
	}, nil
}

// ListTopics - lahko tudi TAIL ali če ni ograničeno
func (n *ChainNode) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	// V verziji 9-10 lahko katerokoli vozlišče služi branjem
	// (ker so vsi sinhronizirani preko CP)
	n.mu.RLock()
	defer n.mu.RUnlock()

	topics := make([]*pb.Topic, 0, len(n.topics))
	for _, topic := range n.topics {
		topics = append(topics, topic)
	}

	log.Printf("[%s] ListTopics: returning %d topics", n.nodeID, len(topics))
	return &pb.ListTopicsResponse{Topics: topics}, nil
}

// GetMessages - lahko tudi TAIL ali če ni ograničeno
func (n *ChainNode) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	// V verziji 9-10 lahko katerokoli vozlišče služi branjem
	n.mu.RLock()
	defer n.mu.RUnlock()

	messages, exists := n.messagesByTopic[req.TopicId]
	if !exists {
		log.Printf("[%s] GetMessages: topic %d not found", n.nodeID, req.TopicId)
		return &pb.GetMessagesResponse{Messages: []*pb.Message{}}, nil
	}

	result := make([]*pb.Message, 0)
	for _, msg := range messages {
		if msg.Id >= req.FromMessageId {
			result = append(result, msg)
			if req.Limit > 0 && len(result) >= int(req.Limit) {
				break
			}
		}
	}

	log.Printf("[%s] GetMessages: returning %d messages from topic %d", n.nodeID, len(result), req.TopicId)
	return &pb.GetMessagesResponse{Messages: result}, nil
}

// SubscribeTopic - katerokoli vozlišče lahko streama
func (n *ChainNode) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	log.Printf("[%s] User %d subscribing to topics %v", n.nodeID, req.UserId, req.TopicId)

	eventChan := make(chan *pb.MessageEvent, 100)
	ctx, cancel := context.WithCancel(stream.Context())

	subscription := &Subscription{
		UserID:     req.UserId,
		TopicIDs:   req.TopicId,
		EventChan:  eventChan,
		CancelFunc: cancel,
	}

	n.subscriptionMutex.Lock()
	n.subscriptions[req.UserId] = subscription
	n.subscriptionMutex.Unlock()

	defer func() {
		n.subscriptionMutex.Lock()
		delete(n.subscriptions, req.UserId)
		n.subscriptionMutex.Unlock()
		close(eventChan)
		log.Printf("[%s] User %d unsubscribed", n.nodeID, req.UserId)
	}()

	// Pošlji zgodovinske messages
	if req.FromMessageId > 0 {
		n.sendHistoricalMessages(req, stream)
	}

	// Stream events
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-eventChan:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

// sendHistoricalMessages pošlje pretekle messages
func (n *ChainNode) sendHistoricalMessages(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, topicID := range req.TopicId {
		messages, exists := n.messagesByTopic[topicID]
		if !exists {
			continue
		}

		for _, msg := range messages {
			if msg.Id >= req.FromMessageId {
				event := &pb.MessageEvent{
					SequenceNumber: atomic.AddInt64(&n.nextSeqNumber, 1),
					Op:             pb.OpType_OP_POST,
					Message:        msg,
					EventAt:        msg.CreatedAt,
				}
				stream.Send(event)
			}
		}
	}
}

// notifySubscribers obvesti lokalne naročnike
func (n *ChainNode) notifySubscribers(topicID int64, opType pb.OpType, message *pb.Message) {
	n.subscriptionMutex.RLock()
	defer n.subscriptionMutex.RUnlock()

	event := &pb.MessageEvent{
		SequenceNumber: atomic.AddInt64(&n.nextSeqNumber, 1),
		Op:             opType,
		Message:        message,
		EventAt:        timestamppb.Now(),
	}

	for _, sub := range n.subscriptions {
		subscribed := false
		for _, tid := range sub.TopicIDs {
			if tid == topicID {
				subscribed = true
				break
			}
		}

		if subscribed {
			select {
			case sub.EventChan <- event:
			default:
			}
		}
	}
}

// ============================================================================
// REPLICATION - Methods za propagacijo podatkov
// ============================================================================

func (n *ChainNode) replicateUser(user *pb.User) {
	if n.successorClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := n.successorClient.ReplicateUser(ctx, &pb.ReplicateUserRequest{User: user})
	if err != nil {
		log.Printf("[%s] Failed to replicate user: %v", n.nodeID, err)
	}
}

func (n *ChainNode) replicateTopic(topic *pb.Topic) {
	if n.successorClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := n.successorClient.ReplicateTopic(ctx, &pb.ReplicateTopicRequest{Topic: topic})
	if err != nil {
		log.Printf("[%s] Failed to replicate topic: %v", n.nodeID, err)
	}
}

func (n *ChainNode) replicateMessage(message *pb.Message) {
	if n.successorClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := n.successorClient.ReplicateMessage(ctx, &pb.ReplicateMessageRequest{Message: message})
	if err != nil {
		log.Printf("[%s] Failed to replicate message: %v", n.nodeID, err)
	}
}

func (n *ChainNode) replicateLike(messageID, userID int64, newLikes int32) {
	if n.successorClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := n.successorClient.ReplicateLike(ctx, &pb.ReplicateLikeRequest{
		MessageId:    messageID,
		UserId:       userID,
		NewLikeCount: newLikes,
	})
	if err != nil {
		log.Printf("[%s] Failed to replicate like: %v", n.nodeID, err)
	}
}

// ============================================================================
// REPLICATION SERVICE - Sprejemanje replikacijskih klicev
// ============================================================================

func (n *ChainNode) ReplicateUser(ctx context.Context, req *pb.ReplicateUserRequest) (*emptypb.Empty, error) {
	n.mu.Lock()
	n.users[req.User.Id] = req.User
	n.mu.Unlock()

	log.Printf("[%s] Replicated user: %s (ID: %d)", n.nodeID, req.User.Name, req.User.Id)

	// Propagiraj naprej če imam successorja
	if n.successor != nil {
		log.Printf("[%s] Propagating user %d to successor", n.nodeID, req.User.Id)
		go n.replicateUser(req.User)
	}

	return &emptypb.Empty{}, nil
}

func (n *ChainNode) ReplicateTopic(ctx context.Context, req *pb.ReplicateTopicRequest) (*emptypb.Empty, error) {
	n.mu.Lock()
	n.topics[req.Topic.Id] = req.Topic
	if n.messagesByTopic[req.Topic.Id] == nil {
		n.messagesByTopic[req.Topic.Id] = make([]*pb.Message, 0)
	}
	n.mu.Unlock()

	log.Printf("[%s] Replicated topic: %s (ID: %d)", n.nodeID, req.Topic.Name, req.Topic.Id)

	// Propagiraj naprej če imam successorja
	if n.successor != nil {
		log.Printf("[%s] Propagating topic %d to successor", n.nodeID, req.Topic.Id)
		go n.replicateTopic(req.Topic)
	}

	return &emptypb.Empty{}, nil
}

func (n *ChainNode) ReplicateMessage(ctx context.Context, req *pb.ReplicateMessageRequest) (*emptypb.Empty, error) {
	n.mu.Lock()
	n.messages[req.Message.Id] = req.Message
	n.messagesByTopic[req.Message.TopicId] = append(n.messagesByTopic[req.Message.TopicId], req.Message)
	n.mu.Unlock()

	log.Printf("[%s] Replicated message %d to topic %d", n.nodeID, req.Message.Id, req.Message.TopicId)

	// Obvesti lokalne naročnike
	go n.notifySubscribers(req.Message.TopicId, pb.OpType_OP_POST, req.Message)

	// POMEMBNO: Propagiraj naprej če nisem TAIL
	if n.successor != nil {
		log.Printf("[%s] Propagating message %d to successor", n.nodeID, req.Message.Id)
		go n.replicateMessage(req.Message)
	} else {
		log.Printf("[%s] I am TAIL, not propagating further", n.nodeID)
	}

	return &emptypb.Empty{}, nil
}

func (n *ChainNode) ReplicateLike(ctx context.Context, req *pb.ReplicateLikeRequest) (*emptypb.Empty, error) {
	n.mu.Lock()
	message, exists := n.messages[req.MessageId]
	if exists {
		message.Likes = req.NewLikeCount
	}
	n.mu.Unlock()

	if exists {
		log.Printf("[%s] Replicated like for message %d (total: %d)", n.nodeID, req.MessageId, req.NewLikeCount)

		// Obvesti lokalne naročnike
		go n.notifySubscribers(message.TopicId, pb.OpType_OP_LIKE, message)

		// Propagiraj naprej če imam successorja
		if n.successor != nil {
			log.Printf("[%s] Propagating like for message %d to successor", n.nodeID, req.MessageId)
			go n.replicateLike(req.MessageId, req.UserId, req.NewLikeCount)
		}
	}

	return &emptypb.Empty{}, nil
}

func (n *ChainNode) UpdateSuccessor(ctx context.Context, req *pb.UpdateSuccessorRequest) (*emptypb.Empty, error) {
	log.Printf("[%s] Updating successor to: %s", n.nodeID, req.Successor.NodeId)
	n.SetSuccessor(req.Successor.Address)
	return &emptypb.Empty{}, nil
}

func (n *ChainNode) Ping(ctx context.Context, req *emptypb.Empty) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		NodeId: n.nodeID,
		Status: "OK",
	}, nil
}

func (n *ChainNode) GetFullState(ctx context.Context, req *emptypb.Empty) (*pb.FullStateResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	users := make([]*pb.User, 0, len(n.users))
	for _, u := range n.users {
		users = append(users, u)
	}

	topics := make([]*pb.Topic, 0, len(n.topics))
	for _, t := range n.topics {
		topics = append(topics, t)
	}

	messages := make([]*pb.Message, 0, len(n.messages))
	for _, m := range n.messages {
		messages = append(messages, m)
	}

	log.Printf("[%s] GetFullState: returning %d users, %d topics, %d messages",
		n.nodeID, len(users), len(topics), len(messages))

	return &pb.FullStateResponse{
		Users:    users,
		Topics:   topics,
		Messages: messages,
	}, nil
}

func (n *ChainNode) UpdatePredecessor(ctx context.Context, req *pb.UpdatePredecessorRequest) (*emptypb.Empty, error) {
	if req.Predecessor != nil {
		log.Printf("[%s] Updating predecessor to: %s", n.nodeID, req.Predecessor.NodeId)
		n.predecessor = req.Predecessor
	} else {
		log.Printf("[%s] Clearing predecessor", n.nodeID)
		n.predecessor = nil
	}
	return &emptypb.Empty{}, nil
}

func (n *ChainNode) SyncState(ctx context.Context, req *pb.FullStateResponse) (*emptypb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[%s] Syncing state: %d users, %d topics, %d messages",
		n.nodeID, len(req.Users), len(req.Topics), len(req.Messages))

	// Apliciraj users
	for _, user := range req.Users {
		n.users[user.Id] = user
	}

	// Apliciraj topics
	for _, topic := range req.Topics {
		n.topics[topic.Id] = topic
		if n.messagesByTopic[topic.Id] == nil {
			n.messagesByTopic[topic.Id] = make([]*pb.Message, 0)
		}
	}

	// Apliciraj messages
	for _, msg := range req.Messages {
		n.messages[msg.Id] = msg
		// Dodaj v messagesByTopic če še ni
		found := false
		for _, existing := range n.messagesByTopic[msg.TopicId] {
			if existing.Id == msg.Id {
				found = true
				break
			}
		}
		if !found {
			n.messagesByTopic[msg.TopicId] = append(n.messagesByTopic[msg.TopicId], msg)
		}
	}

	log.Printf("[%s] State sync completed", n.nodeID)
	return &emptypb.Empty{}, nil
}

// RegisterWithControlPlane registrira vozlišče v kontrolni ravnini
func (n *ChainNode) RegisterWithControlPlane() error {
	if n.controlPlaneAddr == "" {
		return fmt.Errorf("control plane address not set")
	}

	// Poveži se
	conn, err := grpc.Dial(n.controlPlaneAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect to control plane: %v", err)
	}

	n.controlPlaneConn = conn
	n.controlPlaneClient = pb.NewNodeManagementClient(conn)

	// Registriraj
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := n.controlPlaneClient.RegisterNode(ctx, &pb.RegisterNodeRequest{
		Node: &pb.NodeInfo{
			NodeId:  n.nodeID,
			Address: n.address,
		},
	})
	if err != nil {
		return fmt.Errorf("registration failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Message)
	}

	log.Printf("[%s] Registered with control plane", n.nodeID)

	// Posodobi chain info
	if resp.ChainInfo != nil {
		n.updateChainInfo(resp.ChainInfo)
	}

	// Zaženi heartbeat
	go n.heartbeatLoop()

	return nil
}

// heartbeatLoop periodično pošilja heartbeat
func (n *ChainNode) heartbeatLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := n.controlPlaneClient.Heartbeat(ctx, &pb.HeartbeatRequest{
				NodeId: n.nodeID,
			})
			cancel()

			if err != nil {
				log.Printf("[%s] Heartbeat failed: %v", n.nodeID, err)
			}
		case <-n.heartbeatStop:
			return
		}
	}
}

// updateChainInfo posodobi chain informacije
func (n *ChainNode) updateChainInfo(info *pb.ChainInfoResponse) {
	n.chainMutex.Lock()
	defer n.chainMutex.Unlock()

	n.chainNodes = info.AllNodes

	// Določi vlogo na podlagi pozicije
	if info.Head != nil && info.Head.NodeId == n.nodeID {
		n.role = RoleHead
		log.Printf("[%s] Role updated to HEAD", n.nodeID)
	} else if info.Tail != nil && info.Tail.NodeId == n.nodeID {
		n.role = RoleTail
		log.Printf("[%s] Role updated to TAIL", n.nodeID)
	} else {
		n.role = RoleMiddle
		log.Printf("[%s] Role updated to MIDDLE", n.nodeID)
	}
}

// GetClusterState vrne stanje cluster-ja
func (n *ChainNode) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	// Pridobi info iz control plane
	if n.controlPlaneClient != nil {
		ctx2, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		info, err := n.controlPlaneClient.GetChainInfo(ctx2, &emptypb.Empty{})
		if err == nil && info != nil {
			return &pb.GetClusterStateResponse{
				Head: info.Head,
				Tail: info.Tail,
			}, nil
		}
	}

	// Fallback - uporabi lokalne podatke
	n.chainMutex.RLock()
	defer n.chainMutex.RUnlock()

	var headInfo, tailInfo *pb.NodeInfo
	if len(n.chainNodes) > 0 {
		headInfo = n.chainNodes[0]
		tailInfo = n.chainNodes[len(n.chainNodes)-1]
	}

	return &pb.GetClusterStateResponse{
		Head: headInfo,
		Tail: tailInfo,
	}, nil
}

// Stop zaustavlja node
func (n *ChainNode) Stop() {
	close(n.heartbeatStop)
	if n.controlPlaneConn != nil {
		n.controlPlaneConn.Close()
	}
	if n.successorConn != nil {
		n.successorConn.Close()
	}
}
