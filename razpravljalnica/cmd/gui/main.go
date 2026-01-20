package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"

	"razpravljalnica/client"
	pb "razpravljalnica/proto"
)

type GUI struct {
	client *client.Client
	server *http.Server

	// State
	currentUser  *pb.User
	currentTopic *pb.Topic
	topics       []*pb.Topic
	messages     []*pb.Message
	mu           sync.RWMutex

	// SSE clients for real-time updates
	sseClients map[chan string]bool
	sseMu      sync.RWMutex

	// Subscription
	subscriptionActive bool
	subscriptionCancel context.CancelFunc
}

type PageData struct {
	User     *pb.User
	Topic    *pb.Topic
	Topics   []*pb.Topic
	Messages []*pb.Message
	Status   string
}

type MessageJSON struct {
	ID        int64  `json:"id"`
	TopicID   int64  `json:"topic_id"`
	UserID    int64  `json:"user_id"`
	Text      string `json:"text"`
	Likes     int32  `json:"likes"`
	CreatedAt string `json:"created_at"`
}

type TopicJSON struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type UserJSON struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func NewGUI(serverAddr string) (*GUI, error) {
	c, err := client.NewClient(serverAddr)
	if err != nil {
		return nil, err
	}

	gui := &GUI{
		client:             c,
		topics:             make([]*pb.Topic, 0),
		messages:           make([]*pb.Message, 0),
		sseClients:         make(map[chan string]bool),
		subscriptionActive: false,
	}

	return gui, nil
}

func (g *GUI) setupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Serve the main page
	mux.HandleFunc("/", g.handleIndex)

	// API endpoints
	mux.HandleFunc("/api/login", g.handleLogin)
	mux.HandleFunc("/api/topics", g.handleTopics)
	mux.HandleFunc("/api/topics/create", g.handleCreateTopic)
	mux.HandleFunc("/api/topics/select", g.handleSelectTopic)
	mux.HandleFunc("/api/messages", g.handleMessages)
	mux.HandleFunc("/api/messages/send", g.handleSendMessage)
	mux.HandleFunc("/api/messages/like", g.handleLikeMessage)
	mux.HandleFunc("/api/subscribe", g.handleSubscribe)
	mux.HandleFunc("/api/events", g.handleSSE)

	return mux
}

func (g *GUI) handleIndex(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Razpravljalnica Chat</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #eee;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .header {
            background: rgba(0,0,0,0.3);
            padding: 15px 20px;
            display: flex;
            align-items: center;
            gap: 15px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .header h1 {
            font-size: 1.5em;
            color: #4fc3f7;
        }
        .header-buttons {
            display: flex;
            gap: 10px;
            margin-left: auto;
        }
        .user-info {
            color: #81c784;
            font-weight: bold;
        }
        .main-container {
            display: flex;
            flex: 1;
            overflow: hidden;
        }
        .sidebar {
            width: 280px;
            background: rgba(0,0,0,0.2);
            border-right: 1px solid rgba(255,255,255,0.1);
            display: flex;
            flex-direction: column;
        }
        .sidebar-header {
            padding: 15px;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .sidebar-header h2 {
            font-size: 1.1em;
            color: #aaa;
        }
        .topic-list {
            flex: 1;
            overflow-y: auto;
            padding: 10px;
        }
        .topic-item {
            padding: 12px 15px;
            margin-bottom: 5px;
            background: rgba(255,255,255,0.05);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
        }
        .topic-item:hover {
            background: rgba(79, 195, 247, 0.2);
        }
        .topic-item.active {
            background: rgba(79, 195, 247, 0.3);
            border-left: 3px solid #4fc3f7;
        }
        .chat-area {
            flex: 1;
            display: flex;
            flex-direction: column;
        }
        .chat-header {
            padding: 15px 20px;
            background: rgba(0,0,0,0.2);
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .chat-header h2 {
            color: #4fc3f7;
        }
        .messages {
            flex: 1;
            overflow-y: auto;
            padding: 20px;
        }
        .message {
            margin-bottom: 15px;
            padding: 12px 15px;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            max-width: 80%;
        }
        .message.own {
            margin-left: auto;
            background: rgba(79, 195, 247, 0.2);
        }
        .message-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
            font-size: 0.85em;
            color: #888;
        }
        .message-user {
            color: #4fc3f7;
            font-weight: bold;
        }
        .message-text {
            line-height: 1.4;
        }
        .message-footer {
            display: flex;
            justify-content: flex-end;
            margin-top: 8px;
        }
        .like-btn {
            background: none;
            border: none;
            color: #e91e63;
            cursor: pointer;
            font-size: 0.9em;
            padding: 5px 10px;
            border-radius: 15px;
            transition: all 0.2s;
        }
        .like-btn:hover {
            background: rgba(233, 30, 99, 0.2);
        }
        .input-area {
            padding: 15px 20px;
            background: rgba(0,0,0,0.3);
            display: flex;
            gap: 10px;
        }
        .input-area input {
            flex: 1;
            padding: 12px 15px;
            border: none;
            border-radius: 25px;
            background: rgba(255,255,255,0.1);
            color: #fff;
            font-size: 1em;
        }
        .input-area input::placeholder {
            color: #666;
        }
        .input-area input:focus {
            outline: none;
            background: rgba(255,255,255,0.15);
        }
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 20px;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
        }
        .btn-primary {
            background: #4fc3f7;
            color: #000;
        }
        .btn-primary:hover {
            background: #29b6f6;
        }
        .btn-secondary {
            background: rgba(255,255,255,0.1);
            color: #fff;
        }
        .btn-secondary:hover {
            background: rgba(255,255,255,0.2);
        }
        .status-bar {
            padding: 8px 20px;
            background: rgba(0,0,0,0.4);
            font-size: 0.85em;
            color: #888;
        }
        .status-bar.success {
            color: #81c784;
        }
        .status-bar.error {
            color: #e57373;
        }
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.7);
            justify-content: center;
            align-items: center;
            z-index: 1000;
        }
        .modal.active {
            display: flex;
        }
        .modal-content {
            background: #1a1a2e;
            padding: 30px;
            border-radius: 15px;
            min-width: 350px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        .modal-content h3 {
            margin-bottom: 20px;
            color: #4fc3f7;
        }
        .modal-content input {
            width: 100%;
            padding: 12px 15px;
            margin-bottom: 15px;
            border: 1px solid rgba(255,255,255,0.2);
            border-radius: 8px;
            background: rgba(255,255,255,0.05);
            color: #fff;
            font-size: 1em;
        }
        .modal-content input:focus {
            outline: none;
            border-color: #4fc3f7;
        }
        .modal-buttons {
            display: flex;
            gap: 10px;
            justify-content: flex-end;
        }
        .welcome-message {
            text-align: center;
            padding: 50px;
            color: #666;
        }
        .welcome-message h2 {
            color: #4fc3f7;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>💬 Razpravljalnica</h1>
        <div class="header-buttons">
            <button class="btn btn-primary" onclick="showLoginModal()">Login</button>
            <button class="btn btn-secondary" onclick="showCreateTopicModal()">New Topic</button>
            <button class="btn btn-secondary" onclick="refreshTopics()">Refresh Topics</button>
            <button class="btn btn-secondary" onclick="loadMessages()">Refresh Messages</button>
            <label style="color:#aaa;display:flex;align-items:center;gap:5px;">
                <input type="checkbox" id="autoRefresh" onchange="toggleAutoRefresh()" checked> Auto-refresh
            </label>
        </div>
        <span class="user-info" id="userInfo">Not logged in</span>
    </div>

    <div class="main-container">
        <div class="sidebar">
            <div class="sidebar-header">
                <h2>Topics</h2>
            </div>
            <div class="topic-list" id="topicList">
                <div class="welcome-message">
                    <p>No topics yet</p>
                </div>
            </div>
        </div>

        <div class="chat-area">
            <div class="chat-header">
                <h2 id="chatTitle">Select a topic to start chatting</h2>
            </div>
            <div class="messages" id="messages">
                <div class="welcome-message">
                    <h2>Welcome to Razpravljalnica!</h2>
                    <p>1. Click "Login" to create your user</p>
                    <p>2. Create a new topic or select an existing one</p>
                    <p>3. Start chatting!</p>
                </div>
            </div>
            <div class="input-area">
                <input type="text" id="messageInput" placeholder="Type your message..." onkeypress="handleKeyPress(event)">
                <button class="btn btn-primary" onclick="sendMessage()">Send</button>
            </div>
        </div>
    </div>

    <div class="status-bar" id="statusBar">Ready</div>

    <!-- Login Modal -->
    <div class="modal" id="loginModal">
        <div class="modal-content">
            <h3>Register / Login</h3>
            <div style="margin-bottom:15px;">
                <label style="color:#aaa;font-size:0.9em;">Register new user:</label>
                <input type="text" id="loginName" placeholder="Enter your name" style="margin-top:5px;">
                <button class="btn btn-primary" onclick="register()" style="width:100%;margin-top:5px;">Register</button>
            </div>
            <div style="border-top:1px solid rgba(255,255,255,0.1);padding-top:15px;">
                <label style="color:#aaa;font-size:0.9em;">Login with existing User ID:</label>
                <div style="display:flex;gap:10px;margin-top:5px;">
                    <input type="number" id="loginId" placeholder="User ID" style="flex:1;">
                    <input type="text" id="loginIdName" placeholder="Name (optional)" style="flex:1;">
                </div>
                <button class="btn btn-secondary" onclick="loginById()" style="width:100%;margin-top:5px;">Login by ID</button>
            </div>
            <div class="modal-buttons" style="margin-top:15px;">
                <button class="btn btn-secondary" onclick="closeModal('loginModal')">Cancel</button>
            </div>
        </div>
    </div>

    <!-- Create Topic Modal -->
    <div class="modal" id="createTopicModal">
        <div class="modal-content">
            <h3>Create New Topic</h3>
            <input type="text" id="topicName" placeholder="Enter topic name">
            <div class="modal-buttons">
                <button class="btn btn-secondary" onclick="closeModal('createTopicModal')">Cancel</button>
                <button class="btn btn-primary" onclick="createTopic()">Create</button>
            </div>
        </div>
    </div>

    <script>
        let currentUser = null;
        let currentTopic = null;
        let eventSource = null;

        function setStatus(message, type = '') {
            const statusBar = document.getElementById('statusBar');
            statusBar.textContent = message;
            statusBar.className = 'status-bar ' + type;
        }

        function showLoginModal() {
            document.getElementById('loginModal').classList.add('active');
            document.getElementById('loginName').focus();
        }

        function showCreateTopicModal() {
            if (!currentUser) {
                setStatus('Please login first', 'error');
                return;
            }
            document.getElementById('createTopicModal').classList.add('active');
            document.getElementById('topicName').focus();
        }

        function closeModal(id) {
            document.getElementById(id).classList.remove('active');
        }

        async function register() {
            const name = document.getElementById('loginName').value.trim();
            if (!name) {
                setStatus('Please enter a name', 'error');
                return;
            }

            try {
                const response = await fetch('/api/login', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: name })
                });
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                currentUser = data;
                document.getElementById('userInfo').textContent = 'Logged in as: ' + data.name + ' (ID: ' + data.id + ')';
                closeModal('loginModal');
                setStatus('Registered and logged in as ' + data.name + ' (ID: ' + data.id + ') - Remember your ID to login later!', 'success');
                refreshTopics();
            } catch (err) {
                setStatus('Registration failed: ' + err.message, 'error');
            }
        }

        function loginById() {
            const id = document.getElementById('loginId').value.trim();
            const name = document.getElementById('loginIdName').value.trim() || 'User ' + id;
            
            if (!id) {
                setStatus('Please enter a User ID', 'error');
                return;
            }

            const userId = parseInt(id);
            if (isNaN(userId) || userId <= 0) {
                setStatus('Please enter a valid User ID (positive number)', 'error');
                return;
            }

            currentUser = { id: userId, name: name };
            document.getElementById('userInfo').textContent = 'Logged in as: ' + name + ' (ID: ' + userId + ')';
            closeModal('loginModal');
            setStatus('Logged in as ' + name + ' (ID: ' + userId + ')', 'success');
            refreshTopics();
        }

        async function createTopic() {
            const name = document.getElementById('topicName').value.trim();
            if (!name) {
                setStatus('Please enter a topic name', 'error');
                return;
            }

            try {
                const response = await fetch('/api/topics/create', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: name })
                });
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                closeModal('createTopicModal');
                document.getElementById('topicName').value = '';
                setStatus('Topic "' + data.name + '" created!', 'success');
                refreshTopics();
            } catch (err) {
                setStatus('Failed to create topic: ' + err.message, 'error');
            }
        }

        async function refreshTopics() {
            try {
                const response = await fetch('/api/topics');
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                const topicList = document.getElementById('topicList');
                if (data.length === 0) {
                    topicList.innerHTML = '<div class="welcome-message"><p>No topics yet</p></div>';
                } else {
                    topicList.innerHTML = data.map(topic => 
                        '<div class="topic-item' + (currentTopic && currentTopic.id === topic.id ? ' active' : '') + 
                        '" onclick="selectTopic(' + topic.id + ', \'' + escapeHtml(topic.name) + '\')">' + 
                        escapeHtml(topic.name) + '</div>'
                    ).join('');
                }
                setStatus('Loaded ' + data.length + ' topics', 'success');
            } catch (err) {
                setStatus('Failed to load topics: ' + err.message, 'error');
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        async function selectTopic(id, name) {
            currentTopic = { id: id, name: name };
            document.getElementById('chatTitle').textContent = name;
            
            // Update active state
            document.querySelectorAll('.topic-item').forEach(item => {
                item.classList.remove('active');
            });
            event.target.classList.add('active');

            try {
                const response = await fetch('/api/topics/select?id=' + id);
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                loadMessages();
            } catch (err) {
                setStatus('Failed to select topic: ' + err.message, 'error');
            }
        }

        async function loadMessages() {
            if (!currentTopic) return;

            try {
                const response = await fetch('/api/messages?topic_id=' + currentTopic.id);
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                renderMessages(data);
                setStatus('Viewing: ' + currentTopic.name + ' (' + data.length + ' messages)', 'success');
            } catch (err) {
                setStatus('Failed to load messages: ' + err.message, 'error');
            }
        }

        function renderMessages(messages) {
            const container = document.getElementById('messages');
            if (messages.length === 0) {
                container.innerHTML = '<div class="welcome-message"><p>No messages yet. Be the first to post!</p></div>';
                return;
            }

            container.innerHTML = messages.map(msg => {
                const isOwn = currentUser && msg.user_id === currentUser.id;
                return '<div class="message' + (isOwn ? ' own' : '') + '">' +
                    '<div class="message-header">' +
                    '<span class="message-user">User ' + msg.user_id + '</span>' +
                    '<span>' + msg.created_at + '</span>' +
                    '</div>' +
                    '<div class="message-text">' + escapeHtml(msg.text) + '</div>' +
                    '<div class="message-footer">' +
                    '<button class="like-btn" onclick="likeMessage(' + msg.id + ')">❤ ' + msg.likes + '</button>' +
                    '</div>' +
                    '</div>';
            }).join('');

            container.scrollTop = container.scrollHeight;
        }

        function handleKeyPress(event) {
            if (event.key === 'Enter') {
                sendMessage();
            }
        }

        async function sendMessage() {
            if (!currentUser) {
                setStatus('Please login first', 'error');
                return;
            }
            if (!currentTopic) {
                setStatus('Please select a topic first', 'error');
                return;
            }

            const input = document.getElementById('messageInput');
            const text = input.value.trim();
            if (!text) return;

            try {
                const response = await fetch('/api/messages/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ text: text })
                });
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                input.value = '';
                loadMessages();
                setStatus('Message sent!', 'success');
            } catch (err) {
                setStatus('Failed to send message: ' + err.message, 'error');
            }
        }

        async function likeMessage(msgId) {
            if (!currentUser) {
                setStatus('Please login first', 'error');
                return;
            }

            try {
                const response = await fetch('/api/messages/like', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message_id: msgId })
                });
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                loadMessages();
                setStatus('Message liked!', 'success');
            } catch (err) {
                setStatus('Failed to like message: ' + err.message, 'error');
            }
        }

        async function subscribe() {
            if (!currentUser) {
                setStatus('Please login first', 'error');
                return;
            }
            if (!currentTopic) {
                setStatus('Please select a topic first', 'error');
                return;
            }

            try {
                const response = await fetch('/api/subscribe', {
                    method: 'POST'
                });
                const data = await response.json();
                
                if (data.error) {
                    setStatus(data.error, 'error');
                    return;
                }

                // Start SSE connection
                if (eventSource) {
                    eventSource.close();
                }
                eventSource = new EventSource('/api/events');
                eventSource.onmessage = function(event) {
                    const data = JSON.parse(event.data);
                    if (data.type === 'message' && currentTopic && data.topic_id === currentTopic.id) {
                        loadMessages();
                        setStatus('New message received!', 'success');
                    }
                };
                eventSource.onerror = function() {
                    setStatus('Subscription connection lost', 'error');
                };

                setStatus('Subscribed to ' + currentTopic.name + ' (real-time updates)', 'success');
            } catch (err) {
                setStatus('Failed to subscribe: ' + err.message, 'error');
            }
        }

        // Handle Enter key in modals
        document.getElementById('loginName').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') register();
        });
        document.getElementById('loginId').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') loginById();
        });
        document.getElementById('loginIdName').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') loginById();
        });
        document.getElementById('topicName').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') createTopic();
        });

        // Auto-refresh functionality
        let autoRefreshInterval = null;
        
        function toggleAutoRefresh() {
            const checkbox = document.getElementById('autoRefresh');
            if (checkbox.checked) {
                startAutoRefresh();
            } else {
                stopAutoRefresh();
            }
        }
        
        function startAutoRefresh() {
            if (autoRefreshInterval) return;
            autoRefreshInterval = setInterval(() => {
                if (currentTopic) {
                    loadMessages();
                }
            }, 2000); // Refresh every 2 seconds
            console.log('Auto-refresh started');
        }
        
        function stopAutoRefresh() {
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
                autoRefreshInterval = null;
                console.log('Auto-refresh stopped');
            }
        }

        // Initial load
        refreshTopics();
        
        // Start auto-refresh by default
        startAutoRefresh();
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func (g *GUI) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.jsonError(w, "Invalid request")
		return
	}

	user, err := g.client.CreateUser(req.Name)
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Login failed: %v", err))
		return
	}

	g.mu.Lock()
	g.currentUser = user
	g.mu.Unlock()

	g.jsonResponse(w, UserJSON{ID: user.Id, Name: user.Name})
}

func (g *GUI) handleTopics(w http.ResponseWriter, r *http.Request) {
	topics, err := g.client.ListTopics()
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Failed to load topics: %v", err))
		return
	}

	g.mu.Lock()
	g.topics = topics
	g.mu.Unlock()

	result := make([]TopicJSON, len(topics))
	for i, t := range topics {
		result[i] = TopicJSON{ID: t.Id, Name: t.Name}
	}

	g.jsonResponse(w, result)
}

func (g *GUI) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.jsonError(w, "Invalid request")
		return
	}

	topic, err := g.client.CreateTopic(req.Name)
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Failed to create topic: %v", err))
		return
	}

	g.jsonResponse(w, TopicJSON{ID: topic.Id, Name: topic.Name})
}

func (g *GUI) handleSelectTopic(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		g.jsonError(w, "Invalid topic ID")
		return
	}

	g.mu.Lock()
	for _, t := range g.topics {
		if t.Id == id {
			g.currentTopic = t
			break
		}
	}
	g.mu.Unlock()

	g.jsonResponse(w, map[string]string{"status": "ok"})
}

func (g *GUI) handleMessages(w http.ResponseWriter, r *http.Request) {
	topicIDStr := r.URL.Query().Get("topic_id")
	topicID, err := strconv.ParseInt(topicIDStr, 10, 64)
	if err != nil {
		g.jsonError(w, "Invalid topic ID")
		return
	}

	messages, err := g.client.GetMessages(topicID, 0, 100)
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Failed to load messages: %v", err))
		return
	}

	g.mu.Lock()
	g.messages = messages
	g.mu.Unlock()

	result := make([]MessageJSON, len(messages))
	for i, m := range messages {
		result[i] = MessageJSON{
			ID:        m.Id,
			TopicID:   m.TopicId,
			UserID:    m.UserId,
			Text:      m.Text,
			Likes:     m.Likes,
			CreatedAt: m.CreatedAt.AsTime().Format("15:04:05"),
		}
	}

	g.jsonResponse(w, result)
}

func (g *GUI) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	g.mu.RLock()
	user := g.currentUser
	topic := g.currentTopic
	g.mu.RUnlock()

	if user == nil {
		g.jsonError(w, "Please login first")
		return
	}
	if topic == nil {
		g.jsonError(w, "Please select a topic first")
		return
	}

	var req struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.jsonError(w, "Invalid request")
		return
	}

	msg, err := g.client.PostMessage(topic.Id, user.Id, req.Text)
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Failed to send message: %v", err))
		return
	}

	// Notify SSE clients
	g.broadcastSSE(fmt.Sprintf(`{"type":"message","topic_id":%d}`, topic.Id))

	g.jsonResponse(w, MessageJSON{
		ID:        msg.Id,
		TopicID:   msg.TopicId,
		UserID:    msg.UserId,
		Text:      msg.Text,
		Likes:     msg.Likes,
		CreatedAt: msg.CreatedAt.AsTime().Format("15:04:05"),
	})
}

func (g *GUI) handleLikeMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	g.mu.RLock()
	user := g.currentUser
	topic := g.currentTopic
	g.mu.RUnlock()

	if user == nil {
		g.jsonError(w, "Please login first")
		return
	}
	if topic == nil {
		g.jsonError(w, "Please select a topic first")
		return
	}

	var req struct {
		MessageID int64 `json:"message_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		g.jsonError(w, "Invalid request")
		return
	}

	msg, err := g.client.LikeMessage(topic.Id, req.MessageID, user.Id)
	if err != nil {
		g.jsonError(w, fmt.Sprintf("Failed to like message: %v", err))
		return
	}

	// Notify SSE clients
	g.broadcastSSE(fmt.Sprintf(`{"type":"like","topic_id":%d,"message_id":%d}`, topic.Id, req.MessageID))

	g.jsonResponse(w, MessageJSON{
		ID:        msg.Id,
		TopicID:   msg.TopicId,
		UserID:    msg.UserId,
		Text:      msg.Text,
		Likes:     msg.Likes,
		CreatedAt: msg.CreatedAt.AsTime().Format("15:04:05"),
	})
}

func (g *GUI) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	g.mu.RLock()
	user := g.currentUser
	topic := g.currentTopic
	g.mu.RUnlock()

	if user == nil {
		g.jsonError(w, "Please login first")
		return
	}
	if topic == nil {
		g.jsonError(w, "Please select a topic first")
		return
	}

	if g.subscriptionActive {
		g.jsonResponse(w, map[string]string{"status": "already subscribed"})
		return
	}

	g.subscriptionActive = true
	ctx, cancel := context.WithCancel(context.Background())
	g.subscriptionCancel = cancel

	go func() {
		defer func() {
			g.subscriptionActive = false
		}()

		// Get subscription node
		subNodeResp, err := g.client.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
			UserId:  user.Id,
			TopicId: []int64{topic.Id},
		})
		if err != nil {
			log.Printf("Subscription error: %v", err)
			return
		}

		// Subscribe
		stream, err := g.client.SubscribeToTopic(ctx, &pb.SubscribeTopicRequest{
			TopicId:        []int64{topic.Id},
			UserId:         user.Id,
			FromMessageId:  0,
			SubscribeToken: subNodeResp.SubscribeToken,
		})
		if err != nil {
			log.Printf("Subscription error: %v", err)
			return
		}

		// Listen for events
		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, err := stream.Recv()
				if err != nil {
					log.Printf("Subscription ended: %v", err)
					return
				}

				// Broadcast to SSE clients
				g.broadcastSSE(fmt.Sprintf(`{"type":"message","topic_id":%d,"message_id":%d}`,
					event.Message.TopicId, event.Message.Id))
			}
		}
	}()

	g.jsonResponse(w, map[string]string{"status": "subscribed"})
}

func (g *GUI) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Create channel for this client
	messageChan := make(chan string, 10)

	g.sseMu.Lock()
	g.sseClients[messageChan] = true
	g.sseMu.Unlock()

	defer func() {
		g.sseMu.Lock()
		delete(g.sseClients, messageChan)
		g.sseMu.Unlock()
		close(messageChan)
	}()

	// Send initial ping
	fmt.Fprintf(w, "data: {\"type\":\"ping\"}\n\n")
	flusher.Flush()

	for {
		select {
		case msg := <-messageChan:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		case <-time.After(30 * time.Second):
			// Keep-alive ping
			fmt.Fprintf(w, "data: {\"type\":\"ping\"}\n\n")
			flusher.Flush()
		}
	}
}

func (g *GUI) broadcastSSE(message string) {
	g.sseMu.RLock()
	defer g.sseMu.RUnlock()

	for ch := range g.sseClients {
		select {
		case ch <- message:
		default:
			// Channel full, skip
		}
	}
}

func (g *GUI) jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (g *GUI) jsonError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func openBrowser(url string) {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	}
	if err != nil {
		log.Printf("Failed to open browser: %v", err)
	}
}

func (g *GUI) Run(port int) error {
	mux := g.setupRoutes()
	g.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	url := fmt.Sprintf("http://localhost:%d", port)
	log.Printf("Starting GUI server at %s", url)

	// Open browser after a short delay
	go func() {
		time.Sleep(500 * time.Millisecond)
		openBrowser(url)
	}()

	return g.server.ListenAndServe()
}

func main() {
	serverAddr := "localhost:50051"
	guiPort := 8080

	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}
	if len(os.Args) > 2 {
		if p, err := strconv.Atoi(os.Args[2]); err == nil {
			guiPort = p
		}
	}

	gui, err := NewGUI(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create GUI: %v", err)
	}

	if err := gui.Run(guiPort); err != nil && err != http.ErrServerClosed {
		log.Fatalf("GUI server error: %v", err)
	}
}
