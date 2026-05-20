package transport

import "sync"

// MemPersister is an in-memory Persister for use in tests.
type MemPersister struct {
	mu   sync.Mutex
	data []byte
}

func (p *MemPersister) Save(data []byte) {
	p.mu.Lock()
	p.data = make([]byte, len(data))
	copy(p.data, data)
	p.mu.Unlock()
}

func (p *MemPersister) Load() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.data == nil {
		return nil
	}
	out := make([]byte, len(p.data))
	copy(out, p.data)
	return out
}
