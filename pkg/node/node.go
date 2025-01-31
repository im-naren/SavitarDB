package node

import (
    "errors"
    "sync"

    "github.com/im-naren/savitar/pkg/shard"
    "github.com/im-naren/savitar/pkg/vector"
)

// Node represents a node in the distributed system
type Node struct {
    ID     string
    shards map[string]*shard.Shard
    mu     sync.RWMutex
}

// NewNode initializes a new node with one shard
func NewNode(id string) *Node {
    n := &Node{
        ID:     id,
        shards: make(map[string]*shard.Shard),
    }

    // Add an initial shard to the node (simplified for demo purposes)
    n.AddShard("default-shard", shard.NewShard())

    return n
}

// AddShard adds a new shard to the node
func (n *Node) AddShard(shardID string, s *shard.Shard) {
    n.mu.Lock()
    defer n.mu.Unlock()
    n.shards[shardID] = s
}

// AddVector adds a vector to the appropriate shard
func (n *Node) AddVector(v vector.Vector) error {
    shard := n.selectShard(v.ID)
    if shard == nil {
        return errors.New("no shard found")
    }
    return shard.AddVector(v)
}

// GetVector retrieves a vector from the appropriate shard
func (n *Node) GetVector(id string) (vector.Vector, error) {
    shard := n.selectShard(id)
    if shard == nil {
        return vector.Vector{}, errors.New("no shard found")
    }
    return shard.GetVector(id)
}

// selectShard selects a shard based on the vector ID (simplified for demo purposes)
func (n *Node) selectShard(id string) *shard.Shard {
    n.mu.RLock()
    defer n.mu.RUnlock()

    // Always return the default shard for now
    if len(n.shards) > 0 {
        for _, s := range n.shards {
            return s
        }
    }
    return nil
}