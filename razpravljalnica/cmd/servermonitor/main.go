package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "razpravljalnica/proto"
)

type ServerMonitor struct {
	app    *tview.Application
	client pb.MessageBoardClient
	conn   *grpc.ClientConn

	// Layouts
	mainFlex *tview.Flex

	// Widgets
	statsView    *tview.TextView
	topologyView *tview.TextView
	activityLog  *tview.TextView
	commandBar   *tview.TextView

	// State
	mu           sync.RWMutex
	userCount    int
	topicCount   int
	messageCount int
	logs         []string
	maxLogs      int

	// Update ticker
	updateTicker *time.Ticker
	stopChan     chan struct{}
}

func NewServerMonitor(serverAddr string) (*ServerMonitor, error) {
	conn, err := grpc.Dial(serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	sm := &ServerMonitor{
		app:          tview.NewApplication(),
		client:       pb.NewMessageBoardClient(conn),
		conn:         conn,
		logs:         make([]string, 0),
		maxLogs:      100,
		updateTicker: time.NewTicker(2 * time.Second),
		stopChan:     make(chan struct{}),
	}

	sm.setupUI()
	sm.addLog("✓ Connected to server")

	return sm, nil
}

func (sm *ServerMonitor) setupUI() {
	// Stats View (Top Left)
	sm.statsView = tview.NewTextView().
		SetDynamicColors(true)
	sm.statsView.SetBorder(true).
		SetTitle(" Server Statistics ").
		SetTitleAlign(tview.AlignLeft)

	// Topology View (Top Right)
	sm.topologyView = tview.NewTextView().
		SetDynamicColors(true)
	sm.topologyView.SetBorder(true).
		SetTitle(" Cluster Topology ").
		SetTitleAlign(tview.AlignLeft)

	// Activity Log (Bottom)
	sm.activityLog = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetChangedFunc(func() {
			sm.app.Draw()
		})
	sm.activityLog.SetBorder(true).
		SetTitle(" Activity Log ").
		SetTitleAlign(tview.AlignLeft)

	// Command Bar
	sm.commandBar = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	sm.commandBar.SetText("[yellow]Press [green]R[-yellow] to refresh | [green]Q[-yellow] or [green]Ctrl+C[-yellow] to quit[-]")

	// Top Row (Stats and Topology)
	topRow := tview.NewFlex().
		AddItem(sm.statsView, 0, 1, false).
		AddItem(sm.topologyView, 0, 1, false)

	// Main Layout
	sm.mainFlex = tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(topRow, 0, 1, false).
		AddItem(sm.activityLog, 0, 1, false).
		AddItem(sm.commandBar, 1, 0, false)

	// Keyboard shortcuts
	sm.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyCtrlC:
			sm.app.Stop()
			return nil
		case tcell.KeyRune:
			switch event.Rune() {
			case 'q', 'Q':
				sm.app.Stop()
				return nil
			case 'r', 'R':
				sm.refresh()
				return nil
			}
		}
		return event
	})

	sm.app.SetRoot(sm.mainFlex, true)
}

func (sm *ServerMonitor) refresh() {
	sm.addLog("🔄 Refreshing...")

	// Get cluster state - using ControlPlane client
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Create ControlPlane client
	cpClient := pb.NewControlPlaneClient(sm.conn)
	clusterState, err := cpClient.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		sm.addLog(fmt.Sprintf("[red]Error getting cluster state: %v", err))
		// Continue with basic stats even if cluster state fails
	}

	// Get topics
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()

	topicsResp, err := sm.client.ListTopics(ctx2, &emptypb.Empty{})
	if err != nil {
		sm.addLog(fmt.Sprintf("[red]Error getting topics: %v", err))
		return
	}

	// Get messages from each topic
	totalMessages := 0
	for _, topic := range topicsResp.Topics {
		ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
		messagesResp, err := sm.client.GetMessages(ctx3, &pb.GetMessagesRequest{
			TopicId:       topic.Id,
			FromMessageId: 0,
			Limit:         1000,
		})
		cancel3()

		if err == nil {
			totalMessages += len(messagesResp.Messages)
		}
	}

	sm.mu.Lock()
	sm.topicCount = len(topicsResp.Topics)
	sm.messageCount = totalMessages
	sm.mu.Unlock()

	sm.updateStatsView()
	if clusterState != nil {
		sm.updateTopologyView(clusterState)
	}
	sm.addLog("✓ Refresh complete")
}

func (sm *ServerMonitor) updateStatsView() {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	now := time.Now().Format("15:04:05")

	stats := fmt.Sprintf(
		"[yellow]═══════════════════════════════════════[-]\n"+
			"[cyan]       SERVER METRICS[-]\n"+
			"[yellow]═══════════════════════════════════════[-]\n\n"+
			"[green]📊 Statistics:[-]\n"+
			"  Topics:       [yellow]%d[-]\n"+
			"  Messages:     [yellow]%d[-]\n"+
			"  Users:        [yellow]%d[-]\n\n"+
			"[green]⏰ Last Update:[-] [gray]%s[-]\n",
		sm.topicCount,
		sm.messageCount,
		sm.userCount,
		now,
	)

	sm.statsView.SetText(stats)
	sm.app.Draw()
}

func (sm *ServerMonitor) updateTopologyView(state *pb.GetClusterStateResponse) {
	topology := "[yellow]═══════════════════════════════════════[-]\n" +
		"[cyan]       CLUSTER TOPOLOGY[-]\n" +
		"[yellow]═══════════════════════════════════════[-]\n\n"

	if state.Head != nil {
		topology += fmt.Sprintf("[green]▶ HEAD:[-] [yellow]%s[-]\n", state.Head.NodeId)
		topology += fmt.Sprintf("  Address: [gray]%s[-]\n\n", state.Head.Address)
	} else {
		topology += "[red]▶ HEAD: Not available[-]\n\n"
	}

	if state.Tail != nil {
		topology += fmt.Sprintf("[green]◀ TAIL:[-] [yellow]%s[-]\n", state.Tail.NodeId)
		topology += fmt.Sprintf("  Address: [gray]%s[-]\n\n", state.Tail.Address)
	} else {
		topology += "[red]◀ TAIL: Not available[-]\n\n"
	}

	// Check if single node or chain
	if state.Head != nil && state.Tail != nil {
		if state.Head.NodeId == state.Tail.NodeId {
			topology += "[cyan]Mode: Single Node[-]\n"
		} else {
			topology += "[cyan]Mode: Chain Replication[-]\n"
		}
	}

	sm.topologyView.SetText(topology)
	sm.app.Draw()
}

func (sm *ServerMonitor) addLog(message string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	timestamp := time.Now().Format("15:04:05")
	logEntry := fmt.Sprintf("[gray]%s[-] %s\n", timestamp, message)

	sm.logs = append(sm.logs, logEntry)
	if len(sm.logs) > sm.maxLogs {
		sm.logs = sm.logs[1:]
	}

	// Update log view
	sm.activityLog.Clear()
	for _, entry := range sm.logs {
		fmt.Fprint(sm.activityLog, entry)
	}
	sm.activityLog.ScrollToEnd()
	sm.app.Draw()
}

func (sm *ServerMonitor) startAutoRefresh() {
	go func() {
		for {
			select {
			case <-sm.updateTicker.C:
				sm.refresh()
			case <-sm.stopChan:
				return
			}
		}
	}()
}

func (sm *ServerMonitor) Run() error {
	// Initial refresh
	sm.refresh()

	// Start auto-refresh
	sm.startAutoRefresh()

	// Show welcome message
	sm.addLog("[green]═══════════════════════════════════════[-]")
	sm.addLog("[cyan]  Razpravljalnica Server Monitor[-]")
	sm.addLog("[green]═══════════════════════════════════════[-]")
	sm.addLog("")
	sm.addLog("[yellow]Auto-refresh every 2 seconds[-]")
	sm.addLog("[yellow]Press R to refresh manually[-]")
	sm.addLog("")

	return sm.app.Run()
}

func (sm *ServerMonitor) Stop() {
	close(sm.stopChan)
	sm.updateTicker.Stop()
	if sm.conn != nil {
		sm.conn.Close()
	}
}

func main() {
	serverAddr := "localhost:50051"
	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}

	monitor, err := NewServerMonitor(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create server monitor: %v", err)
	}
	defer monitor.Stop()

	if err := monitor.Run(); err != nil {
		log.Fatalf("Monitor error: %v", err)
	}
}
