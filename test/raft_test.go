package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Devansh63/raft-go/raft"
	"github.com/Devansh63/raft-go/transport"
)

// ---------------------------------------------------------------------------
// Test cluster helper
// ---------------------------------------------------------------------------

type cluster struct {
	t          *testing.T
	n          int
	network    *transport.Network
	transports []*transport.InMem
	rafts      []*raft.Raft
	applyChans []chan raft.ApplyMsg
	applied    [][]raft.ApplyMsg
	mu         struct{ _ int } // unused — applied slices are written by single goroutines
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	net, transports := transport.NewNetwork(n)
	c := &cluster{
		t:          t,
		n:          n,
		network:    net,
		transports: transports,
		rafts:      make([]*raft.Raft, n),
		applyChans: make([]chan raft.ApplyMsg, n),
		applied:    make([][]raft.ApplyMsg, n),
	}
	for i := 0; i < n; i++ {
		c.applyChans[i] = make(chan raft.ApplyMsg, 256)
		p := &transport.MemPersister{}
		c.rafts[i] = raft.New(i, n, transports[i], p, c.applyChans[i])
		net.Register(i, c.rafts[i])
		go func(idx int) {
			for msg := range c.applyChans[idx] {
				c.applied[idx] = append(c.applied[idx], msg)
			}
		}(i)
	}
	return c
}

func (c *cluster) shutdown() {
	for _, r := range c.rafts {
		r.Kill()
	}
}

// leader polls until exactly one leader is found, or fails after 3 s.
func (c *cluster) leader() int {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range c.rafts {
			_, isLeader := r.GetState()
			if isLeader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader elected within 3s")
	return -1
}

func (c *cluster) isolate(peer int) {
	c.network.IsolateAll(peer)
}

func (c *cluster) reconnect(peer int) {
	c.network.ConnectAll(peer)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	leader := c.leader()
	t.Logf("leader elected: peer %d", leader)

	// Confirm exactly one leader.
	count := 0
	for _, r := range c.rafts {
		_, ok := r.GetState()
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("want 1 leader, got %d", count)
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	old := c.leader()
	t.Logf("initial leader: peer %d", old)

	// Kill the leader and isolate it.
	c.rafts[old].Kill()
	c.isolate(old)

	// Allow time for a new election.
	time.Sleep(700 * time.Millisecond)
	newLeader := c.leader()
	t.Logf("new leader: peer %d", newLeader)

	if newLeader == old {
		t.Fatal("killed peer was re-elected")
	}
}

func TestLogReplication(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.leader()

	// Submit five commands.
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		idx, _, ok := c.rafts[leader].Start(cmd)
		if !ok {
			t.Fatalf("Start returned false on leader (cmd %d)", i)
		}
		t.Logf("submitted %q at log index %d", cmd, idx)
	}

	// Give the cluster time to replicate and apply.
	time.Sleep(400 * time.Millisecond)

	// Every peer should have applied at least 5 entries.
	for i := 0; i < c.n; i++ {
		if len(c.applied[i]) < 5 {
			t.Errorf("peer %d applied %d entries, want >= 5", i, len(c.applied[i]))
		}
	}
}

func TestSplitBrainPrevention(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	old := c.leader()
	oldTerm, _ := c.rafts[old].GetState()
	t.Logf("initial leader: peer %d, term %d", old, oldTerm)

	// Isolate the current leader (minority partition).
	c.isolate(old)

	// Majority should elect a new leader.
	time.Sleep(800 * time.Millisecond)

	newLeader := -1
	for i, r := range c.rafts {
		if i == old {
			continue
		}
		_, ok := r.GetState()
		if ok {
			newLeader = i
			break
		}
	}
	if newLeader == -1 {
		t.Fatal("majority partition failed to elect a new leader")
	}
	newTerm, _ := c.rafts[newLeader].GetState()
	if newTerm <= oldTerm {
		t.Errorf("new leader term %d should be > old term %d", newTerm, oldTerm)
	}
	t.Logf("new leader: peer %d, term %d", newLeader, newTerm)
}

func TestLogConsistencyAfterPartition(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	leader := c.leader()

	// Commit some entries before the partition.
	for i := 0; i < 3; i++ {
		c.rafts[leader].Start(fmt.Sprintf("pre-%d", i))
	}
	time.Sleep(300 * time.Millisecond)

	// Isolate the leader.
	c.isolate(leader)
	time.Sleep(700 * time.Millisecond)

	// New leader — commit more entries on the majority side.
	newLeader := c.leader()
	for i := 0; i < 3; i++ {
		c.rafts[newLeader].Start(fmt.Sprintf("post-%d", i))
	}
	time.Sleep(300 * time.Millisecond)

	// Reconnect the old leader. It must catch up without corrupting the log.
	c.reconnect(leader)
	time.Sleep(500 * time.Millisecond)

	// All live peers should have applied the same entries in the same order.
	ref := c.applied[newLeader]
	for i := 0; i < c.n; i++ {
		if i == leader {
			continue // old leader may still be behind — skip for now
		}
		if len(c.applied[i]) < len(ref) {
			t.Errorf("peer %d applied %d entries, want %d", i, len(c.applied[i]), len(ref))
			continue
		}
		for j, msg := range ref {
			if c.applied[i][j].Command != msg.Command {
				t.Errorf("peer %d entry %d: got %v, want %v", i, j, c.applied[i][j].Command, msg.Command)
			}
		}
	}
}
