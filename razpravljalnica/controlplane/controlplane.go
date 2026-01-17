package controlplane

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "razpravljalnica/proto"
)

const (
	HeartbeatInterval = 5 * time.Second
	HeartbeatTimeout  = 15 * time.Second
)

// NodeState hrani stanje vozlišča
type NodeState struct {
	Info          *pb.NodeInfo
	Healthy       bool
	LastHeartbeat time.Time
	Successor     string // Node ID naslednika
	Predecessor   string // Node ID predhodnika
	Client        pb.ReplicationServiceClient
	Conn          *grpc.ClientConn
}

// ControlPlane upravlja z verigo vozlišč
type ControlPlane struct {
	pb.UnimplementedControlPlaneServer
	pb.UnimplementedNodeManagementServer

	mu sync.RWMutex

	// Chain state
	nodes map[string]*NodeState // node_id -> NodeState
	head  string                // Node ID glave
	tail  string                // Node ID repa

	// Background workers
	stopChan chan struct{}
}

// NewControlPlane ustvari novo kontrolno ravnino
func NewControlPlane() *ControlPlane {
	cp := &ControlPlane{
		nodes:    make(map[string]*NodeState),
		stopChan: make(chan struct{}),
	}

	// Zaženi health checker
	go cp.healthCheckLoop()

	return cp
}

// RegisterNode registrira novo vozlišče
func (cp *ControlPlane) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	nodeID := req.Node.NodeId
	nodeAddr := req.Node.Address

	log.Printf("[ControlPlane] Registering node: %s at %s", nodeID, nodeAddr)

	// Poveži se na node
	conn, err := grpc.Dial(nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return &pb.RegisterNodeResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to connect: %v", err),
		}, nil
	}

	client := pb.NewReplicationServiceClient(conn)

	// Ping test
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Ping(pingCtx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return &pb.RegisterNodeResponse{
			Success: false,
			Message: fmt.Sprintf("Ping failed: %v", err),
		}, nil
	}

	// Ustvari node state
	state := &NodeState{
		Info:          req.Node,
		Healthy:       true,
		LastHeartbeat: time.Now(),
		Client:        client,
		Conn:          conn,
	}

	cp.nodes[nodeID] = state

	// Dodaj v verigo
	cp.addToChain(nodeID)

	log.Printf("[ControlPlane] Node %s registered successfully. Chain: %s", nodeID, cp.getChainString())

	// Vrni chain info
	chainInfo := cp.getChainInfo()

	return &pb.RegisterNodeResponse{
		Success:   true,
		Message:   "Node registered successfully",
		ChainInfo: chainInfo,
	}, nil
}

// Heartbeat od vozlišča
func (cp *ControlPlane) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	state, exists := cp.nodes[req.NodeId]
	if !exists {
		return &emptypb.Empty{}, fmt.Errorf("unknown node: %s", req.NodeId)
	}

	state.LastHeartbeat = time.Now()
	if !state.Healthy {
		log.Printf("[ControlPlane] Node %s is back online!", req.NodeId)
		state.Healthy = true
	}

	return &emptypb.Empty{}, nil
}

// GetChainInfo vrne informacije o verigi
func (cp *ControlPlane) GetChainInfo(ctx context.Context, req *emptypb.Empty) (*pb.ChainInfoResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return cp.getChainInfo(), nil
}

// GetClusterState implementira ControlPlane service iz osnovnega proto
func (cp *ControlPlane) GetClusterState(ctx context.Context, req *emptypb.Empty) (*pb.GetClusterStateResponse, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	var headInfo, tailInfo *pb.NodeInfo

	if cp.head != "" {
		headInfo = cp.nodes[cp.head].Info
	}
	if cp.tail != "" {
		tailInfo = cp.nodes[cp.tail].Info
	}

	return &pb.GetClusterStateResponse{
		Head: headInfo,
		Tail: tailInfo,
	}, nil
}

// addToChain doda vozlišče v verigo
func (cp *ControlPlane) addToChain(nodeID string) {
	// Če ni nodes, postane HEAD in TAIL
	if cp.head == "" {
		cp.head = nodeID
		cp.tail = nodeID
		log.Printf("[ControlPlane] %s is now HEAD and TAIL", nodeID)
		return
	}

	// Dodaj pred TAIL (na konec verige)
	oldTail := cp.tail
	cp.nodes[oldTail].Successor = nodeID
	cp.nodes[nodeID].Predecessor = oldTail
	cp.tail = nodeID

	log.Printf("[ControlPlane] Added %s to chain before TAIL", nodeID)

	// Posodobi successor na starem TAIL
	go cp.updateNodeSuccessor(oldTail, nodeID)

	// Sinhroniziraj stanje na novi node
	go cp.syncNodeState(nodeID, oldTail)
}

// removeFromChain odstrani vozlišče iz verige
func (cp *ControlPlane) removeFromChain(nodeID string) {
	state := cp.nodes[nodeID]
	pred := state.Predecessor
	succ := state.Successor

	log.Printf("[ControlPlane] Removing %s from chain (pred=%s, succ=%s)", nodeID, pred, succ)

	// Če je HEAD
	if nodeID == cp.head {
		cp.head = succ
		if succ != "" {
			cp.nodes[succ].Predecessor = ""
		}
	}

	// Če je TAIL
	if nodeID == cp.tail {
		cp.tail = pred
		if pred != "" {
			cp.nodes[pred].Successor = ""
		}
	}

	// Če je MIDDLE
	if pred != "" && succ != "" {
		cp.nodes[pred].Successor = succ
		cp.nodes[succ].Predecessor = pred

		// Poveži predecessor → successor
		go cp.updateNodeSuccessor(pred, succ)
		go cp.updateNodePredecessor(succ, pred)
	}

	// Zapri connection
	if state.Conn != nil {
		state.Conn.Close()
	}

	delete(cp.nodes, nodeID)

	log.Printf("[ControlPlane] Node %s removed. New chain: %s", nodeID, cp.getChainString())
}

// updateNodeSuccessor posodobi naslednika na vozlišču
func (cp *ControlPlane) updateNodeSuccessor(nodeID, successorID string) {
	state, exists := cp.nodes[nodeID]
	if !exists {
		return
	}

	var successorInfo *pb.NodeInfo
	if successorID != "" {
		successorInfo = cp.nodes[successorID].Info
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := state.Client.UpdateSuccessor(ctx, &pb.UpdateSuccessorRequest{
		Successor: successorInfo,
	})
	if err != nil {
		log.Printf("[ControlPlane] Failed to update successor on %s: %v", nodeID, err)
	} else {
		log.Printf("[ControlPlane] Updated successor on %s to %s", nodeID, successorID)
	}
}

// updateNodePredecessor posodobi predhodnika na vozlišču
func (cp *ControlPlane) updateNodePredecessor(nodeID, predecessorID string) {
	state, exists := cp.nodes[nodeID]
	if !exists {
		return
	}

	var predecessorInfo *pb.NodeInfo
	if predecessorID != "" {
		predecessorInfo = cp.nodes[predecessorID].Info
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := state.Client.UpdatePredecessor(ctx, &pb.UpdatePredecessorRequest{
		Predecessor: predecessorInfo,
	})
	if err != nil {
		log.Printf("[ControlPlane] Failed to update predecessor on %s: %v", nodeID, err)
	} else {
		log.Printf("[ControlPlane] Updated predecessor on %s to %s", nodeID, predecessorID)
	}
}

// syncNodeState sinhroniziraj stanje na novo vozlišče
func (cp *ControlPlane) syncNodeState(targetNodeID, sourceNodeID string) {
	targetState, exists := cp.nodes[targetNodeID]
	if !exists {
		return
	}

	sourceState, exists := cp.nodes[sourceNodeID]
	if !exists {
		return
	}

	log.Printf("[ControlPlane] Syncing state from %s to %s", sourceNodeID, targetNodeID)

	// Pridobi state iz source
	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()

	state, err := sourceState.Client.GetFullState(ctx1, &emptypb.Empty{})
	if err != nil {
		log.Printf("[ControlPlane] Failed to get state from %s: %v", sourceNodeID, err)
		return
	}

	// Pošlji state na target
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	_, err = targetState.Client.SyncState(ctx2, state)
	if err != nil {
		log.Printf("[ControlPlane] Failed to sync state to %s: %v", targetNodeID, err)
		return
	}

	log.Printf("[ControlPlane] Successfully synced state to %s", targetNodeID)
}

// healthCheckLoop periodično preverja zdravje vozlišč
func (cp *ControlPlane) healthCheckLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cp.checkHealth()
		case <-cp.stopChan:
			return
		}
	}
}

// checkHealth preveri zdravje vseh vozlišč
func (cp *ControlPlane) checkHealth() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	now := time.Now()
	failedNodes := make([]string, 0)

	for nodeID, state := range cp.nodes {
		if !state.Healthy {
			continue
		}

		// Preveri heartbeat timeout
		if now.Sub(state.LastHeartbeat) > HeartbeatTimeout {
			log.Printf("[ControlPlane] ⚠️  Node %s missed heartbeat!", nodeID)

			// Dodatni ping test
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := state.Client.Ping(ctx, &emptypb.Empty{})
			cancel()

			if err != nil {
				log.Printf("[ControlPlane] ❌ Node %s is DOWN: %v", nodeID, err)
				state.Healthy = false
				failedNodes = append(failedNodes, nodeID)
			} else {
				// Ping uspel, posodobi heartbeat
				state.LastHeartbeat = now
			}
		}
	}

	// Odstrani failed nodes
	for _, nodeID := range failedNodes {
		cp.removeFromChain(nodeID)
	}
}

// getChainInfo vrne informacije o verigi (internal)
func (cp *ControlPlane) getChainInfo() *pb.ChainInfoResponse {
	var headInfo, tailInfo *pb.NodeInfo
	allNodes := make([]*pb.NodeInfo, 0, len(cp.nodes))
	successors := make(map[string]string)

	if cp.head != "" {
		headInfo = cp.nodes[cp.head].Info
	}
	if cp.tail != "" {
		tailInfo = cp.nodes[cp.tail].Info
	}

	for nodeID, state := range cp.nodes {
		allNodes = append(allNodes, state.Info)
		if state.Successor != "" {
			successors[nodeID] = state.Successor
		}
	}

	return &pb.ChainInfoResponse{
		Head:           headInfo,
		Tail:           tailInfo,
		AllNodes:       allNodes,
		NodeSuccessors: successors,
	}
}

// getChainString vrne string reprezentacijo verige
func (cp *ControlPlane) getChainString() string {
	if cp.head == "" {
		return "(empty)"
	}

	chain := ""
	current := cp.head
	for current != "" {
		if chain != "" {
			chain += " → "
		}
		chain += current

		state, exists := cp.nodes[current]
		if !exists || state.Successor == "" {
			break
		}
		current = state.Successor
	}

	return chain
}

// Stop zaustavlja kontrolno ravnino
func (cp *ControlPlane) Stop() {
	close(cp.stopChan)

	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, state := range cp.nodes {
		if state.Conn != nil {
			state.Conn.Close()
		}
	}
}
