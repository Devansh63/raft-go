# raft-go

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go. No third-party libraries — only the standard library.

Includes a linearizable key-value store built on top of the Raft layer to demonstrate end-to-end usage.

---

## What's implemented

- **Leader election** — randomized election timeouts, RequestVote RPC, majority quorum
- **Log replication** — AppendEntries RPC, heartbeats, fast log backup on conflict
- **Safety** — election restriction (only up-to-date candidates win), leader completeness
- **Commit detection** — leader advances commitIndex once a quorum has matched an entry, and only for entries in the current term
- **Persistence** — `currentTerm`, `votedFor`, and `log` encoded with `encoding/gob` and saved before any RPC reply
- **Apply loop** — committed entries delivered to the state machine via `chan ApplyMsg`
- **KV store** — linearizable `Get`/`Put`/`Delete` backed by Raft; redirects non-leader requests

---

## How Raft works (brief)

```
Servers are always in one of three states:

  ┌──────────────────────────────────────────────────────────┐
  │  Follower  ──election timeout──>  Candidate              │
  │                                      │                   │
  │                          wins majority vote              │
  │                                      ▼                   │
  │            <──higher term seen──   Leader                │
  │                                      │                   │
  │                              sends AppendEntries         │
  │                             (heartbeats + log entries)   │
  └──────────────────────────────────────────────────────────┘

Log replication (leader perspective):

  client ──> Start(cmd) ──> append to local log
                              │
                        broadcast AppendEntries to all followers
                              │
                        once majority have written it: advance commitIndex
                              │
                        next heartbeat tells followers the new commitIndex
                              │
                        followers apply up to commitIndex via applyCh
```

### Key safety property

A leader **never overwrites or deletes entries in its log**. A candidate can only win an election if its log is at least as up-to-date as a majority of the cluster — guaranteeing that any committed entry will be present in all future leaders.

---

## Locking discipline

Five rules that prevent deadlocks and races:

1. **Lock when accessing shared state.** `currentTerm`, `log`, `role`, `commitIndex` — always behind `mu`.
2. **Release the lock before RPC calls.** RPCs may block; holding the lock during them would deadlock.
3. **Re-check state after re-acquiring the lock.** The peer's role or term may have changed while the lock was released.
4. **Never hold the lock while sending to `applyCh`.** The state machine may block; use a dedicated goroutine and condition variable.
5. **Persist before replying.** `currentTerm`, `votedFor`, and `log` must be written to stable storage before responding to any RPC.

---

## Project structure

```
raft-go/
├── raft/
│   └── raft.go          # complete Raft implementation
├── transport/
│   ├── network.go       # in-memory transport + per-link connectivity matrix
│   └── persister.go     # in-memory Persister for tests
├── kvraft/
│   └── server.go        # linearizable KV store backed by Raft
├── test/
│   └── raft_test.go     # integration tests
└── go.mod
```

---

## Running the tests

```bash
git clone https://github.com/Devansh63/raft-go
cd raft-go
go test ./test/ -v -timeout 60s
```

Test scenarios covered:

| Test | What it verifies |
|------|-----------------|
| `TestLeaderElection` | Exactly one leader elected in a 5-node cluster |
| `TestLeaderFailover` | New leader elected after current leader crashes |
| `TestLogReplication` | Entries committed and applied on all peers |
| `TestSplitBrainPrevention` | Minority partition cannot elect a leader |
| `TestLogConsistencyAfterPartition` | No committed entries lost after network heals |

---

## Key design decisions

**Randomized election timeouts (300–600ms)**: reduces the probability of split votes. Each peer picks a random timeout independently; the first to time out usually wins before others start campaigning.

**Fast log backup**: when a follower rejects an AppendEntries due to a term conflict, it sends back `(XTerm, XIndex, XLen)` so the leader can skip over the entire conflicting term in one round trip rather than decrementing `nextIndex` by 1 per RPC.

**Commit rule**: a leader only advances `commitIndex` for entries with `log[N].term == currentTerm`. This prevents the classic "figure 8" scenario where a leader re-commits an entry from a previous term that was never majority-acknowledged.

**Condition variable for apply**: a `sync.Cond` wakes the applier goroutine whenever `commitIndex` advances, avoiding busy-polling. The applier releases `mu` before sending to `applyCh` to prevent deadlocks with the state machine.

**Persistence before reply**: `persist()` is called inside the lock before any RPC handler returns. If the server crashes and restarts, it recovers `currentTerm` (prevents stale votes), `votedFor` (prevents double-voting), and `log` (prevents log loss).
