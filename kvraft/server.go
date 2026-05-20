// Package kvraft implements a linearizable key-value store backed by Raft.
// Every write (Put/Delete) and read (Get) goes through the Raft log so that
// all replicas observe the same order of operations.
package kvraft

import (
	"errors"
	"sync"

	"github.com/Devansh63/raft-go/raft"
)

// ErrNotLeader is returned when a request is sent to a non-leader peer.
var ErrNotLeader = errors.New("not leader")

// Op is a key-value store command submitted to Raft.
type Op struct {
	Type  string // "Get", "Put", "Delete"
	Key   string
	Value string
}

type result struct {
	value string
	err   error
}

// KVServer is a linearizable key-value store backed by Raft.
type KVServer struct {
	mu     sync.Mutex
	rf     *raft.Raft
	data   map[string]string
	notify map[int]chan result // log index -> waiter channel
}

// New creates a KVServer. applyCh must be the same channel passed to raft.New().
func New(rf *raft.Raft, applyCh chan raft.ApplyMsg) *KVServer {
	kv := &KVServer{
		rf:     rf,
		data:   make(map[string]string),
		notify: make(map[int]chan result),
	}
	go kv.applyLoop(applyCh)
	return kv
}

// Get returns the current value for key, or "" if absent.
// Returns ErrNotLeader if this server is not the Raft leader.
func (kv *KVServer) Get(key string) (string, error) {
	return kv.submit(Op{Type: "Get", Key: key})
}

// Put sets key = value.
func (kv *KVServer) Put(key, value string) error {
	_, err := kv.submit(Op{Type: "Put", Key: key, Value: value})
	return err
}

// Delete removes key from the store.
func (kv *KVServer) Delete(key string) error {
	_, err := kv.submit(Op{Type: "Delete", Key: key})
	return err
}

func (kv *KVServer) submit(op Op) (string, error) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrNotLeader
	}
	ch := make(chan result, 1)
	kv.mu.Lock()
	kv.notify[index] = ch
	kv.mu.Unlock()
	res := <-ch
	return res.value, res.err
}

func (kv *KVServer) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if !msg.CommandValid {
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()
		var res result
		switch op.Type {
		case "Get":
			res.value = kv.data[op.Key]
		case "Put":
			kv.data[op.Key] = op.Value
		case "Delete":
			delete(kv.data, op.Key)
		}
		if ch, ok := kv.notify[msg.CommandIndex]; ok {
			delete(kv.notify, msg.CommandIndex)
			ch <- res
		}
		kv.mu.Unlock()
	}
}
