package master

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"razpravljalnica/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	HeartbeatInterval = 2 * time.Second
	FailureThreshold  = 3 // missed heartbeats before declaring failure
)

// Master je nadzorna ravnina za verižno replikacijo
type Master struct {
	pb.UnimplementedControlPlaneServer

	mu          sync.RWMutex
	chain       []*NodeState // ordered list: head -> ... -> tail
	grpcServer  *grpc.Server
	address     string
	
	// Failure detection
	stopHeartbeat chan struct{}
}

type NodeState struct {
	Info            *pb.NodeInfo
	Client          pb.ChainReplicationClient
	LastHeartbeat   time.Time
	MissedHeartbeats int
	Healthy         bool
}

func NewMaster(address string) *Master {
	return &Master{
		address:       address,
		chain:         make([]*NodeState, 0),
		stopHeartbeat: make(chan struct{}),
	}
}

// Start pokrene master
func (m *Master) Start() error {
	lis, err := net.Listen("tcp", m.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	m.grpcServer = grpc.NewServer()
	pb.RegisterControlPlaneServer(m.grpcServer, m)

	// Start heartbeat checker
	go m.heartbeatLoop()

	log.Printf("Master starting on %s", m.address)
	return m.grpcServer.Serve(lis)
}

// Stop ustavi master
func (m *Master) Stop() {
	close(m.stopHeartbeat)
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
	}
}

// GetClusterState vrne trenutno stanje verige
func (m *Master) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.chain) == 0 {
		return nil, fmt.Errorf("no nodes in chain")
	}

	head := m.chain[0].Info
	tail := m.chain[len(m.chain)-1].Info

	return &pb.GetClusterStateResponse{
		Head: head,
		Tail: tail,
	}, nil
}

// AddNode doda novo vozlišče v verigo
func (m *Master) AddNode(nodeInfo *pb.NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create gRPC client
	conn, err := grpc.Dial(nodeInfo.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to node: %v", err)
	}

	client := pb.NewChainReplicationClient(conn)

	nodeState := &NodeState{
		Info:            nodeInfo,
		Client:          client,
		LastHeartbeat:   time.Now(),
		MissedHeartbeats: 0,
		Healthy:         true,
	}

	// Add to end of chain (new tail)
	if len(m.chain) == 0 {
		// First node - both head and tail
		m.chain = append(m.chain, nodeState)
		
		update := &pb.TopologyUpdate{
			NodeId:      nodeInfo.NodeId,
			Predecessor: nil,
			Successor:   nil,
			IsHead:      true,
			IsTail:      true,
		}
		
		client.UpdateTopology(context.Background(), update)
		log.Printf("Added first node %s (head & tail)", nodeInfo.NodeId)
		
	} else {
		// Add as new tail
		oldTail := m.chain[len(m.chain)-1]
		
		// Sync state from old tail
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		snapshot, err := oldTail.Client.SyncState(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("failed to sync state: %v", err)
		}
		
		// Restore state on new node
		// (This would require additional RPC method, simplified here)
		
		// Update topology
		m.chain = append(m.chain, nodeState)
		
		// Update old tail
		oldTailUpdate := &pb.TopologyUpdate{
			NodeId:      oldTail.Info.NodeId,
			Predecessor: m.getPredecessor(len(m.chain) - 2),
			Successor:   nodeInfo,
			IsHead:      len(m.chain) == 2, // still head if chain length is 2
			IsTail:      false,
		}
		oldTail.Client.UpdateTopology(context.Background(), oldTailUpdate)
		
		// Update new tail
		newTailUpdate := &pb.TopologyUpdate{
			NodeId:      nodeInfo.NodeId,
			Predecessor: oldTail.Info,
			Successor:   nil,
			IsHead:      false,
			IsTail:      true,
		}
		client.UpdateTopology(context.Background(), newTailUpdate)
		
		log.Printf("Added node %s as new tail", nodeInfo.NodeId)
	}

	return nil
}

// heartbeatLoop periodično preverja zdravje vozlišč
func (m *Master) heartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkHeartbeats()
		case <-m.stopHeartbeat:
			return
		}
	}
}

func (m *Master) checkHeartbeats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, node := range m.chain {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		
		req := &pb.HeartbeatRequest{
			NodeId:    node.Info.NodeId,
			Timestamp: time.Now().Unix(),
		}
		
		_, err := node.Client.Heartbeat(ctx, req)
		cancel()

		if err != nil {
			node.MissedHeartbeats++
			log.Printf("Node %s missed heartbeat (%d/%d)", 
				node.Info.NodeId, node.MissedHeartbeats, FailureThreshold)
			
			if node.MissedHeartbeats >= FailureThreshold {
				node.Healthy = false
				log.Printf("Node %s declared FAILED", node.Info.NodeId)
				
				// Handle failure
				go m.handleNodeFailure(i)
			}
		} else {
			node.MissedHeartbeats = 0
			node.LastHeartbeat = time.Now()
			if !node.Healthy {
				node.Healthy = true
				log.Printf("Node %s recovered", node.Info.NodeId)
			}
		}
	}
}

// handleNodeFailure obravnava odpoved vozlišča
func (m *Master) handleNodeFailure(failedIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if failedIndex < 0 || failedIndex >= len(m.chain) {
		return
	}

	failed := m.chain[failedIndex]
	log.Printf("Handling failure of node %s at position %d/%d", 
		failed.Info.NodeId, failedIndex+1, len(m.chain))

	switch {
	case len(m.chain) == 1:
		// Only node failed - service unavailable
		log.Printf("CRITICAL: Only node failed, service unavailable")
		m.chain = nil

	case failedIndex == 0:
		// Head failed
		m.handleHeadFailure()

	case failedIndex == len(m.chain)-1:
		// Tail failed
		m.handleTailFailure()

	default:
		// Middle node failed
		m.handleMiddleFailure(failedIndex)
	}
}

func (m *Master) handleHeadFailure() {
	log.Printf("Head failure detected")
	
	// Remove old head
	m.chain = m.chain[1:]
	
	if len(m.chain) == 0 {
		return
	}
	
	// New head is the next node
	newHead := m.chain[0]
	
	update := &pb.TopologyUpdate{
		NodeId:      newHead.Info.NodeId,
		Predecessor: nil,
		Successor:   m.getSuccessor(0),
		IsHead:      true,
		IsTail:      len(m.chain) == 1,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if _, err := newHead.Client.UpdateTopology(ctx, update); err != nil {
		log.Printf("Failed to update new head: %v", err)
	} else {
		log.Printf("New head: %s", newHead.Info.NodeId)
	}
}

func (m *Master) handleTailFailure() {
	log.Printf("Tail failure detected")
	
	// Remove old tail
	m.chain = m.chain[:len(m.chain)-1]
	
	if len(m.chain) == 0 {
		return
	}
	
	// New tail is the previous node
	newTail := m.chain[len(m.chain)-1]
	
	update := &pb.TopologyUpdate{
		NodeId:      newTail.Info.NodeId,
		Predecessor: m.getPredecessor(len(m.chain) - 1),
		Successor:   nil,
		IsHead:      len(m.chain) == 1,
		IsTail:      true,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if _, err := newTail.Client.UpdateTopology(ctx, update); err != nil {
		log.Printf("Failed to update new tail: %v", err)
	} else {
		log.Printf("New tail: %s", newTail.Info.NodeId)
	}
}

func (m *Master) handleMiddleFailure(failedIndex int) {
	log.Printf("Middle node failure detected at position %d", failedIndex+1)
	
	// Get neighbors
	predecessor := m.chain[failedIndex-1]
	successor := m.chain[failedIndex+1]
	
	// Get pending updates from predecessor
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// This is simplified - in real implementation, we'd need to:
	// 1. Get Sent_i from predecessor
	// 2. Get last processed seq_num from successor
	// 3. Re-send missing updates
	
	// Remove failed node
	m.chain = append(m.chain[:failedIndex], m.chain[failedIndex+1:]...)
	
	// Update predecessor's successor
	predUpdate := &pb.TopologyUpdate{
		NodeId:      predecessor.Info.NodeId,
		Predecessor: m.getPredecessor(failedIndex - 1),
		Successor:   successor.Info,
		IsHead:      failedIndex-1 == 0,
		IsTail:      false,
	}
	
	if _, err := predecessor.Client.UpdateTopology(ctx, predUpdate); err != nil {
		log.Printf("Failed to update predecessor: %v", err)
	}
	
	// Update successor's predecessor
	succUpdate := &pb.TopologyUpdate{
		NodeId:      successor.Info.NodeId,
		Predecessor: predecessor.Info,
		Successor:   m.getSuccessor(failedIndex),
		IsHead:      false,
		IsTail:      failedIndex == len(m.chain)-1,
	}
	
	if _, err := successor.Client.UpdateTopology(ctx, succUpdate); err != nil {
		log.Printf("Failed to update successor: %v", err)
	}
	
	log.Printf("Reconnected chain: %s <-> %s", 
		predecessor.Info.NodeId, successor.Info.NodeId)
}

// Helper methods
func (m *Master) getPredecessor(index int) *pb.NodeInfo {
	if index <= 0 || index >= len(m.chain) {
		return nil
	}
	return m.chain[index-1].Info
}

func (m *Master) getSuccessor(index int) *pb.NodeInfo {
	if index < 0 || index >= len(m.chain)-1 {
		return nil
	}
	return m.chain[index+1].Info
}

// ListNodes vrne seznam vseh vozlišč (za debugging)
func (m *Master) ListNodes() []*NodeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	nodes := make([]*NodeState, len(m.chain))
	copy(nodes, m.chain)
	return nodes
}
