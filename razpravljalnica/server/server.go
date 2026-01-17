package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "razpravljalnica/proto"
)

// Server implementira MessageBoard service
type Server struct {
	pb.UnimplementedMessageBoardServer
	pb.UnimplementedControlPlaneServer

	mu sync.RWMutex

	// Storage - vse v pomnilniku
	users    map[int64]*pb.User
	topics   map[int64]*pb.Topic
	messages map[int64]*pb.Message

	// Indeksi za hitrejše iskanje
	messagesByTopic map[int64][]*pb.Message // topic_id -> messages

	// Števci za generiranje ID-jev
	nextUserID    int64
	nextTopicID   int64
	nextMessageID int64
	nextSeqNumber int64

	// Subscriptions - user_id -> channels
	subscriptions     map[int64]*Subscription
	subscriptionMutex sync.RWMutex

	// Node info za kontrolno ravnino
	nodeInfo *pb.NodeInfo
}

// Subscription hrani informacije o naročnini uporabnika
type Subscription struct {
	UserID     int64
	TopicIDs   []int64
	EventChan  chan *pb.MessageEvent
	CancelFunc context.CancelFunc
}

// NewServer ustvari nov server
func NewServer(nodeID, address string) *Server {
	return &Server{
		users:           make(map[int64]*pb.User),
		topics:          make(map[int64]*pb.Topic),
		messages:        make(map[int64]*pb.Message),
		messagesByTopic: make(map[int64][]*pb.Message),
		subscriptions:   make(map[int64]*Subscription),
		nextUserID:      0,
		nextTopicID:     0,
		nextMessageID:   0,
		nextSeqNumber:   0,
		nodeInfo: &pb.NodeInfo{
			NodeId:  nodeID,
			Address: address,
		},
	}
}

// CreateUser ustvari novega uporabnika
func (s *Server) CreateUser(ctx context.Context, req *pb.CreateUserRequest) (*pb.User, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := atomic.AddInt64(&s.nextUserID, 1)
	user := &pb.User{
		Id:   id,
		Name: req.Name,
	}

	s.users[id] = user
	fmt.Printf("[CreateUser] Created user: %s (ID: %d)\n", user.Name, user.Id)

	return user, nil
}

// CreateTopic ustvari novo temo
func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	id := atomic.AddInt64(&s.nextTopicID, 1)
	topic := &pb.Topic{
		Id:   id,
		Name: req.Name,
	}

	s.topics[id] = topic
	s.messagesByTopic[id] = make([]*pb.Message, 0)

	fmt.Printf("[CreateTopic] Created topic: %s (ID: %d)\n", topic.Name, topic.Id)

	return topic, nil
}

// PostMessage objavi sporočilo v temo
func (s *Server) PostMessage(ctx context.Context, req *pb.PostMessageRequest) (*pb.Message, error) {
	if req.Text == "" {
		return nil, status.Error(codes.InvalidArgument, "text cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Preveri ali topic obstaja
	if _, exists := s.topics[req.TopicId]; !exists {
		return nil, status.Error(codes.NotFound, "topic not found")
	}

	// Preveri ali user obstaja
	if _, exists := s.users[req.UserId]; !exists {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	// Ustvari sporočilo
	id := atomic.AddInt64(&s.nextMessageID, 1)
	message := &pb.Message{
		Id:        id,
		TopicId:   req.TopicId,
		UserId:    req.UserId,
		Text:      req.Text,
		CreatedAt: timestamppb.Now(),
		Likes:     0,
	}

	s.messages[id] = message
	s.messagesByTopic[req.TopicId] = append(s.messagesByTopic[req.TopicId], message)

	fmt.Printf("[PostMessage] User %d posted message %d to topic %d: %s\n",
		req.UserId, id, req.TopicId, req.Text)

	// Obvesti naročnike
	go s.notifySubscribers(req.TopicId, pb.OpType_OP_POST, message)

	return message, nil
}

// LikeMessage všečka sporočilo
func (s *Server) LikeMessage(ctx context.Context, req *pb.LikeMessageRequest) (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Preveri ali sporočilo obstaja
	message, exists := s.messages[req.MessageId]
	if !exists {
		return nil, status.Error(codes.NotFound, "message not found")
	}

	// Preveri ali user obstaja
	if _, exists := s.users[req.UserId]; !exists {
		return nil, status.Error(codes.NotFound, "user not found")
	}

	// Povečaj število všečkov
	message.Likes++

	fmt.Printf("[LikeMessage] User %d liked message %d (total likes: %d)\n",
		req.UserId, req.MessageId, message.Likes)

	// Obvesti naročnike
	go s.notifySubscribers(message.TopicId, pb.OpType_OP_LIKE, message)

	return message, nil
}

// GetSubscriptionNode vrne vozlišče za naročnino (v tem primeru vedno ta sam)
func (s *Server) GetSubscriptionNode(ctx context.Context, req *pb.SubscriptionNodeRequest) (*pb.SubscriptionNodeResponse, error) {
	// V osnovni verziji nimamo load balancinga, vedno vrnemo ta node
	token := fmt.Sprintf("token-%d-%d", req.UserId, time.Now().Unix())

	return &pb.SubscriptionNodeResponse{
		SubscribeToken: token,
		Node:           s.nodeInfo,
	}, nil
}

// ListTopics vrne seznam vseh tem
func (s *Server) ListTopics(ctx context.Context, req *emptypb.Empty) (*pb.ListTopicsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*pb.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

// GetMessages vrne sporočila iz teme
func (s *Server) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Preveri ali topic obstaja
	messages, exists := s.messagesByTopic[req.TopicId]
	if !exists {
		return nil, status.Error(codes.NotFound, "topic not found")
	}

	// Filtriraj glede na from_message_id in limit
	result := make([]*pb.Message, 0)
	for _, msg := range messages {
		if msg.Id >= req.FromMessageId {
			result = append(result, msg)
			if req.Limit > 0 && len(result) >= int(req.Limit) {
				break
			}
		}
	}

	return &pb.GetMessagesResponse{Messages: result}, nil
}

// SubscribeTopic naroči uporabnika na teme (streaming)
func (s *Server) SubscribeTopic(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) error {
	// Preveri token (v osnovni verziji samo logiramo)
	fmt.Printf("[SubscribeTopic] User %d subscribing to topics %v with token %s\n",
		req.UserId, req.TopicId, req.SubscribeToken)

	// Ustvari channel za events
	eventChan := make(chan *pb.MessageEvent, 100)
	ctx, cancel := context.WithCancel(stream.Context())

	// Registriraj subscription
	subscription := &Subscription{
		UserID:     req.UserId,
		TopicIDs:   req.TopicId,
		EventChan:  eventChan,
		CancelFunc: cancel,
	}

	s.subscriptionMutex.Lock()
	s.subscriptions[req.UserId] = subscription
	s.subscriptionMutex.Unlock()

	// Cleanup ob koncu
	defer func() {
		s.subscriptionMutex.Lock()
		delete(s.subscriptions, req.UserId)
		s.subscriptionMutex.Unlock()
		close(eventChan)
		fmt.Printf("[SubscribeTopic] User %d unsubscribed\n", req.UserId)
	}()

	// Če želimo zgodovinske messages od določenega ID-ja
	if req.FromMessageId > 0 {
		s.sendHistoricalMessages(req, stream)
	}

	// Streamaj nove evente
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

// sendHistoricalMessages pošlje pretekle messages uporabniku
func (s *Server) sendHistoricalMessages(req *pb.SubscribeTopicRequest, stream pb.MessageBoard_SubscribeTopicServer) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, topicID := range req.TopicId {
		messages, exists := s.messagesByTopic[topicID]
		if !exists {
			continue
		}

		for _, msg := range messages {
			if msg.Id >= req.FromMessageId {
				event := &pb.MessageEvent{
					SequenceNumber: atomic.AddInt64(&s.nextSeqNumber, 1),
					Op:             pb.OpType_OP_POST,
					Message:        msg,
					EventAt:        msg.CreatedAt,
				}
				stream.Send(event)
			}
		}
	}
}

// notifySubscribers obvesti vse naročnike o novem eventu
func (s *Server) notifySubscribers(topicID int64, opType pb.OpType, message *pb.Message) {
	s.subscriptionMutex.RLock()
	defer s.subscriptionMutex.RUnlock()

	event := &pb.MessageEvent{
		SequenceNumber: atomic.AddInt64(&s.nextSeqNumber, 1),
		Op:             opType,
		Message:        message,
		EventAt:        timestamppb.Now(),
	}

	for _, sub := range s.subscriptions {
		// Preveri ali je topic v subscription
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
				// Event poslan
			default:
				// Channel poln, preskočimo
				fmt.Printf("[Warning] Event channel full for user %d\n", sub.UserID)
			}
		}
	}
}

// GetClusterState vrne stanje cluster-ja (za kontrolno ravnino)
func (s *Server) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	// V osnovni verziji smo samo en node - head in tail smo mi
	return &pb.GetClusterStateResponse{
		Head: s.nodeInfo,
		Tail: s.nodeInfo,
	}, nil
}
