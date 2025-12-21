package storage

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrUserNotFound    = errors.New("user not found")
	ErrTopicNotFound   = errors.New("topic not found")
	ErrMessageNotFound = errors.New("message not found")
	ErrInvalidRequest  = errors.New("invalid request")
)

type User struct {
	ID   int64
	Name string
}

type Topic struct {
	ID   int64
	Name string
}

type Message struct {
	ID        int64
	TopicID   int64
	UserID    int64
	Text      string
	CreatedAt time.Time
	Likes     int32
}

// Storage je thread-safe in-memory storage za razpravljalnico
type Storage struct {
	mu sync.RWMutex

	users   map[int64]*User
	topics  map[int64]*Topic
	
	// messages je mapa: topic_id -> mapa: message_id -> Message
	messages map[int64]map[int64]*Message
	
	// likes je mapa: topic_id -> message_id -> set user_id-jev
	likes map[int64]map[int64]map[int64]bool

	// Števci za generiranje ID-jev
	lastUserID    int64
	lastTopicID   int64
	lastMessageID map[int64]int64 // topic_id -> zadnji message_id
	
	// Sequence number za verižno replikacijo
	sequenceNumber int64
}

func NewStorage() *Storage {
	return &Storage{
		users:          make(map[int64]*User),
		topics:         make(map[int64]*Topic),
		messages:       make(map[int64]map[int64]*Message),
		likes:          make(map[int64]map[int64]map[int64]bool),
		lastMessageID:  make(map[int64]int64),
		sequenceNumber: 0,
	}
}

// CreateUser ustvari novega uporabnika
func (s *Storage) CreateUser(name string) (*User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastUserID++
	user := &User{
		ID:   s.lastUserID,
		Name: name,
	}
	s.users[user.ID] = user
	s.sequenceNumber++
	
	return user, nil
}

// GetUser vrne uporabnika po ID-ju
func (s *Storage) GetUser(userID int64) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, exists := s.users[userID]
	if !exists {
		return nil, ErrUserNotFound
	}
	return user, nil
}

// CreateTopic ustvari novo temo
func (s *Storage) CreateTopic(name string) (*Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastTopicID++
	topic := &Topic{
		ID:   s.lastTopicID,
		Name: name,
	}
	s.topics[topic.ID] = topic
	s.messages[topic.ID] = make(map[int64]*Message)
	s.likes[topic.ID] = make(map[int64]map[int64]bool)
	s.lastMessageID[topic.ID] = 0
	s.sequenceNumber++
	
	return topic, nil
}

// ListTopics vrne vse teme
func (s *Storage) ListTopics() []*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}
	return topics
}

// PostMessage objavi sporočilo v temi
func (s *Storage) PostMessage(topicID, userID int64, text string) (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Preveri ali tema obstaja
	if _, exists := s.topics[topicID]; !exists {
		return nil, ErrTopicNotFound
	}

	// Preveri ali uporabnik obstaja
	if _, exists := s.users[userID]; !exists {
		return nil, ErrUserNotFound
	}

	// Generiraj message ID za to temo
	s.lastMessageID[topicID]++
	messageID := s.lastMessageID[topicID]

	message := &Message{
		ID:        messageID,
		TopicID:   topicID,
		UserID:    userID,
		Text:      text,
		CreatedAt: time.Now(),
		Likes:     0,
	}

	s.messages[topicID][messageID] = message
	s.likes[topicID][messageID] = make(map[int64]bool)
	s.sequenceNumber++
	
	return message, nil
}

// GetMessages vrne sporočila iz teme
func (s *Storage) GetMessages(topicID, fromMessageID int64, limit int32) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.topics[topicID]; !exists {
		return nil, ErrTopicNotFound
	}

	topicMessages := s.messages[topicID]
	messages := make([]*Message, 0)

	// Zberi sporočila od fromMessageID naprej
	for id := fromMessageID; id <= s.lastMessageID[topicID]; id++ {
		if msg, exists := topicMessages[id]; exists {
			messages = append(messages, msg)
			if limit > 0 && len(messages) >= int(limit) {
				break
			}
		}
	}

	return messages, nil
}

// LikeMessage všečka sporočilo
func (s *Storage) LikeMessage(topicID, messageID, userID int64) (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topicID]; !exists {
		return nil, ErrTopicNotFound
	}

	message, exists := s.messages[topicID][messageID]
	if !exists {
		return nil, ErrMessageNotFound
	}

	if _, exists := s.users[userID]; !exists {
		return nil, ErrUserNotFound
	}

	// Dodaj like (če še ni všečkal)
	if !s.likes[topicID][messageID][userID] {
		s.likes[topicID][messageID][userID] = true
		message.Likes++
		s.sequenceNumber++
	}

	return message, nil
}

// GetSequenceNumber vrne trenutni sequence number
func (s *Storage) GetSequenceNumber() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequenceNumber
}

// SetSequenceNumber nastavi sequence number (za sinhronizacijo)
func (s *Storage) SetSequenceNumber(seqNum int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequenceNumber = seqNum
}

// GetSnapshot vrne snapshot celotnega stanja (za recovery)
type Snapshot struct {
	Users          map[int64]*User
	Topics         map[int64]*Topic
	Messages       map[int64]map[int64]*Message
	LastUserID     int64
	LastTopicID    int64
	LastMessageID  map[int64]int64
	SequenceNumber int64
}

func (s *Storage) GetSnapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Deep copy
	snapshot := &Snapshot{
		Users:          make(map[int64]*User),
		Topics:         make(map[int64]*Topic),
		Messages:       make(map[int64]map[int64]*Message),
		LastUserID:     s.lastUserID,
		LastTopicID:    s.lastTopicID,
		LastMessageID:  make(map[int64]int64),
		SequenceNumber: s.sequenceNumber,
	}

	for id, user := range s.users {
		snapshot.Users[id] = &User{ID: user.ID, Name: user.Name}
	}

	for id, topic := range s.topics {
		snapshot.Topics[id] = &Topic{ID: topic.ID, Name: topic.Name}
	}

	for topicID, topicMsgs := range s.messages {
		snapshot.Messages[topicID] = make(map[int64]*Message)
		for msgID, msg := range topicMsgs {
			snapshot.Messages[topicID][msgID] = &Message{
				ID:        msg.ID,
				TopicID:   msg.TopicID,
				UserID:    msg.UserID,
				Text:      msg.Text,
				CreatedAt: msg.CreatedAt,
				Likes:     msg.Likes,
			}
		}
	}

	for topicID, lastID := range s.lastMessageID {
		snapshot.LastMessageID[topicID] = lastID
	}

	return snapshot
}

// RestoreFromSnapshot obnovi stanje iz snapshota
func (s *Storage) RestoreFromSnapshot(snapshot *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.users = snapshot.Users
	s.topics = snapshot.Topics
	s.messages = snapshot.Messages
	s.lastUserID = snapshot.LastUserID
	s.lastTopicID = snapshot.LastTopicID
	s.lastMessageID = snapshot.LastMessageID
	s.sequenceNumber = snapshot.SequenceNumber

	// Reconstruct likes from messages
	s.likes = make(map[int64]map[int64]map[int64]bool)
	for topicID := range s.messages {
		s.likes[topicID] = make(map[int64]map[int64]bool)
		for msgID := range s.messages[topicID] {
			s.likes[topicID][msgID] = make(map[int64]bool)
		}
	}
}
