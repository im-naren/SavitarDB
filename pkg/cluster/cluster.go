// Package cluster manages the coordination of nodes in a distributed system
package cluster

import (
	"errors"
	"fmt"
	"github.com/im-naren/savitar/pkg/node"
	"github.com/im-naren/savitar/pkg/vector"
	"sync"
)

// ClusterManager coordinates distributed nodes and handles operations
type ClusterManager struct {
	nodes []*node.Node
	mu    sync.RWMutex
}

// NewClusterManager initializes a cluster manager with a specified number of nodes
func NewClusterManager(nodeCount int) *ClusterManager {
	nodes := make([]*node.Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = node.NewNode(fmt.Sprintf("node-%d", i))
	}
	return &ClusterManager{nodes: nodes}
}

// AddVector adds a vector to the cluster by selecting an appropriate node
func (cm *ClusterManager) AddVector(v vector.Vector) error {
	node := cm.selectNode(v.ID)
	if node == nil {
		return errors.New("no node available")
	}
	return node.AddVector(v)
}

// GetVector retrieves a vector from the appropriate node
func (cm *ClusterManager) GetVector(id string) (vector.Vector, error) {
	node := cm.selectNode(id)
	if node == nil {
		return vector.Vector{}, errors.New("vector not found")
	}
	return node.GetVector(id)
}

// selectNode selects a node based on a hash of the vector ID (simple round-robin demo)
func (cm *ClusterManager) selectNode(id string) *node.Node {
	hash := int(len(id)) % len(cm.nodes)
	return cm.nodes[hash]
}


