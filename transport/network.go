// Package transport provides an in-memory Raft transport for testing.
// Links between peers can be individually connected or disconnected to simulate
// network partitions without spinning up real TCP sockets.
package transport

import (
	"sync"

	"github.com/Devansh63/raft-go/raft"
)

// Server is implemented by raft.Raft to receive incoming RPCs.
type Server interface {
	HandleRequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply)
	HandleAppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply)
}

// Network holds the shared connectivity matrix and server table.
type Network struct {
	mu        sync.RWMutex
	servers   []Server
	connected [][]bool // connected[from][to] = can from send to to
}

// InMem is the Transport implementation for a single peer in the Network.
type InMem struct {
	from    int
	network *Network
}

// NewNetwork creates a Network and one InMem transport per peer.
// All links start connected.
func NewNetwork(n int) (*Network, []*InMem) {
	net := &Network{
		servers:   make([]Server, n),
		connected: make([][]bool, n),
	}
	for i := range net.connected {
		net.connected[i] = make([]bool, n)
		for j := range net.connected[i] {
			net.connected[i][j] = true
		}
	}
	transports := make([]*InMem, n)
	for i := range transports {
		transports[i] = &InMem{from: i, network: net}
	}
	return net, transports
}

// Register assigns the Raft server that handles incoming RPCs for peer i.
func (n *Network) Register(i int, s Server) {
	n.mu.Lock()
	n.servers[i] = s
	n.mu.Unlock()
}

// Disconnect cuts the directed link from -> to (simulates a one-way partition).
// Call Disconnect(a, b) and Disconnect(b, a) to fully isolate two peers.
func (n *Network) Disconnect(from, to int) {
	n.mu.Lock()
	n.connected[from][to] = false
	n.mu.Unlock()
}

// Connect restores the directed link from -> to.
func (n *Network) Connect(from, to int) {
	n.mu.Lock()
	n.connected[from][to] = true
	n.mu.Unlock()
}

// IsolateAll cuts all links to and from peer i.
func (n *Network) IsolateAll(i int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for j := range n.connected {
		n.connected[i][j] = false
		n.connected[j][i] = false
	}
}

// ConnectAll restores all links to and from peer i.
func (n *Network) ConnectAll(i int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for j := range n.connected {
		n.connected[i][j] = true
		n.connected[j][i] = true
	}
}

func (t *InMem) SendRequestVote(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	t.network.mu.RLock()
	ok := t.network.connected[t.from][peer]
	srv := t.network.servers[peer]
	t.network.mu.RUnlock()
	if !ok || srv == nil {
		return false
	}
	srv.HandleRequestVote(args, reply)
	return true
}

func (t *InMem) SendAppendEntries(peer int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	t.network.mu.RLock()
	ok := t.network.connected[t.from][peer]
	srv := t.network.servers[peer]
	t.network.mu.RUnlock()
	if !ok || srv == nil {
		return false
	}
	srv.HandleAppendEntries(args, reply)
	return true
}
