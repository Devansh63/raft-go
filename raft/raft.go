// Package raft implements the Raft consensus algorithm.
// Reference: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014).
package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Role is the current state of a Raft peer.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is delivered to the state machine when an entry is committed.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// Transport abstracts outgoing RPCs so the core algorithm is testable without a network.
type Transport interface {
	SendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
}

// Persister abstracts durable storage for crash recovery.
type Persister interface {
	Save(data []byte)
	Load() []byte
}

// RequestVoteArgs is the RequestVote RPC payload.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC payload (also used for heartbeats).
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the AppendEntries RPC response.
// XTerm/XIndex/XLen enable fast log backup: the leader skips over a conflicting
// term in one round trip instead of decrementing nextIndex one step at a time.
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // term of conflicting entry; -1 if follower log is too short
	XIndex  int // first index with XTerm in follower's log
	XLen    int // follower's log length (used when XTerm == -1)
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Raft struct
// ---------------------------------------------------------------------------

// Raft is a single peer in a Raft cluster.
type Raft struct {
	mu        sync.Mutex
	me        int
	nPeers    int
	transport Transport
	persister Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	dead      int32

	// Persistent state — must be saved before responding to any RPC.
	currentTerm int
	votedFor    int // -1 means not voted this term
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders (reinitialized after each election win).
	nextIndex  []int
	matchIndex []int

	// Election timer.
	lastHeartbeat   time.Time
	electionTimeout time.Duration
}

// New creates a Raft peer, restores any persisted state, and starts background goroutines.
func New(me, nPeers int, t Transport, p Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:          me,
		nPeers:      nPeers,
		transport:   t,
		persister:   p,
		applyCh:     applyCh,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0}}, // index-0 sentinel keeps math simple
		commitIndex: 0,
		lastApplied: 0,
		role:        Follower,
		nextIndex:   make([]int, nPeers),
		matchIndex:  make([]int, nPeers),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	if data := p.Load(); len(data) > 0 {
		rf.readPersist(data)
	}

	go rf.ticker()
	go rf.applier()
	return rf
}

// Kill stops the peer. Safe to call multiple times.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// GetState returns the current term and whether this peer believes it is leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start submits a command to the Raft log. Returns (logIndex, term, isLeader).
// If this server is not the leader, returns immediately with isLeader=false.
func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)

	go rf.broadcastAppendEntries()
	return len(rf.log) - 1, rf.currentTerm, true
}

// ---------------------------------------------------------------------------
// Timer and main loop
// ---------------------------------------------------------------------------

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
	jitter := rand.Int63n(int64(electionTimeoutMax - electionTimeoutMin))
	rf.electionTimeout = electionTimeoutMin + time.Duration(jitter)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		elapsed := time.Since(rf.lastHeartbeat)
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		switch role {
		case Follower, Candidate:
			if elapsed >= timeout {
				rf.startElection()
			}
		case Leader:
			rf.broadcastAppendEntries()
		}

		time.Sleep(heartbeatInterval / 2)
	}
}

// ---------------------------------------------------------------------------
// State transitions (must hold mu)
// ---------------------------------------------------------------------------

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	for i := 0; i < rf.nPeers; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	log.Printf("[peer %d] became leader for term %d", rf.me, rf.currentTerm)
}

// ---------------------------------------------------------------------------
// Leader election
// ---------------------------------------------------------------------------

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	term := rf.currentTerm
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term
	rf.mu.Unlock()

	votes := 1
	majority := rf.nPeers/2 + 1
	var mu sync.Mutex
	var once sync.Once

	for peer := 0; peer < rf.nPeers; peer++ {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.transport.SendRequestVote(p, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				mu.Lock()
				votes++
				won := votes >= majority
				mu.Unlock()
				if won {
					once.Do(func() {
						rf.becomeLeader()
						go rf.broadcastAppendEntries()
					})
				}
			}
		}(peer)
	}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// Already voted for a different candidate this term.
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	// Election restriction: candidate's log must be at least as up-to-date as ours.
	// Higher last term wins; ties broken by log length.
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term
	logOK := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if logOK {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

// ---------------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------------

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for peer := 0; peer < rf.nPeers; peer++ {
		if peer == rf.me {
			continue
		}
		go rf.sendAppendEntries(peer, term)
	}
}

func (rf *Raft) sendAppendEntries(peer, term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if next > len(rf.log) {
		next = len(rf.log)
	}
	prevIdx := next - 1
	prevTerm := rf.log[prevIdx].Term
	entries := make([]LogEntry, len(rf.log[next:]))
	copy(entries, rf.log[next:])
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.transport.SendAppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := prevIdx + len(entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
			rf.nextIndex[peer] = newMatch + 1
			rf.maybeAdvanceCommit()
		}
	} else {
		// Fast log backup.
		if reply.XTerm == -1 {
			// Follower log is too short.
			rf.nextIndex[peer] = reply.XLen
		} else {
			// Search for the last leader entry with XTerm.
			leaderHasTerm := -1
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.XTerm {
					leaderHasTerm = i
					break
				}
			}
			if leaderHasTerm != -1 {
				rf.nextIndex[peer] = leaderHasTerm + 1
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Valid AppendEntries from current leader — reset election timer.
	rf.role = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Log consistency check: prevLogIndex must exist.
	if args.PrevLogIndex >= len(rf.log) {
		reply.XLen = len(rf.log)
		return
	}

	// Log consistency check: terms must match at prevLogIndex.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		for i, e := range rf.log {
			if e.Term == reply.XTerm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	// Append new entries, truncating any conflicting suffix.
	for i, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				rf.log = append(rf.log[:pos], args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	// Advance commit index.
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit >= len(rf.log) {
			newCommit = len(rf.log) - 1
		}
		rf.commitIndex = newCommit
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

// maybeAdvanceCommit advances commitIndex when a quorum has replicated an entry.
// Commit rule: only entries from the current term may be directly committed
// (entries from prior terms are committed implicitly). Must hold mu.
func (rf *Raft) maybeAdvanceCommit() {
	matches := make([]int, rf.nPeers)
	copy(matches, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))
	quorum := matches[rf.nPeers/2]
	if quorum > rf.commitIndex && rf.log[quorum].Term == rf.currentTerm {
		rf.commitIndex = quorum
		rf.applyCond.Broadcast()
	}
}

// ---------------------------------------------------------------------------
// Apply loop
// ---------------------------------------------------------------------------

// applier delivers committed log entries to the state machine.
// Runs as a background goroutine; uses a condition variable to avoid busy-polling.
// Releases mu before sending to applyCh to avoid deadlocking the state machine.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex
		entries := make([]LogEntry, end-start+1)
		copy(entries, rf.log[start:end+1])
		rf.mu.Unlock()

		for i, e := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: start + i,
				CommandTerm:  e.Term,
			}
		}

		rf.mu.Lock()
		if end > rf.lastApplied {
			rf.lastApplied = end
		}
		rf.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

type persistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

// persist encodes durable state and writes it via the Persister.
// Must be called inside mu and before replying to any RPC.
func (rf *Raft) persist() {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistedState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}); err != nil {
		log.Panicf("raft[%d]: persist: %v", rf.me, err)
	}
	rf.persister.Save(buf.Bytes())
}

// readPersist decodes and restores persisted state after a crash.
func (rf *Raft) readPersist(data []byte) {
	var state persistedState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		log.Panicf("raft[%d]: readPersist: %v", rf.me, err)
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
}
