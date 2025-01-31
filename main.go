package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Vector represents a vector with an ID, data points, and metadata
type Vector struct {
	ID   string
	Data []float64
	Meta map[string]string
}

// Node represents a node in the distributed cluster
type Node struct {
	ID     string
	shards map[string]*Shard
	mu     sync.RWMutex
}

// Shard stores a partition of the vector data within a node
type Shard struct {
	vectors map[string]Vector
	mu      sync.RWMutex
}

// ClusterManager manages distributed nodes and coordinates operations
type ClusterManager struct {
	nodes []*Node
	mu    sync.RWMutex
}

// NewClusterManager initializes a new cluster manager with nodes
func NewClusterManager(nodeCount int) *ClusterManager {
	nodes := make([]*Node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		shards := make(map[string]*Shard)
		shards[fmt.Sprintf("shard-%d", i)] = &Shard{vectors: make(map[string]Vector)}
		nodes[i] = &Node{ID: fmt.Sprintf("node-%d", i), shards: shards}
	}
	return &ClusterManager{nodes: nodes}
}

// AddVector adds a vector to a node and shard in the cluster
func (cm *ClusterManager) AddVector(v Vector) error {
	node := cm.selectNode(v.ID)
	shard := node.selectShard(v.ID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.vectors[v.ID]; exists {
		return errors.New("vector with this ID already exists")
	}
	shard.vectors[v.ID] = v
	return nil
}

// GetVector retrieves a vector by its ID from the appropriate node and shard
func (cm *ClusterManager) GetVector(id string) (Vector, error) {
	node := cm.selectNode(id)
	shard := node.selectShard(id)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	v, exists := shard.vectors[id]
	if !exists {
		return Vector{}, errors.New("vector not found")
	}
	return v, nil
}

// SearchVectors performs a distributed search across nodes and shards
func (cm *ClusterManager) SearchVectors(query []float64, topN int, filters map[string]string) ([]Vector, error) {
	resultsChan := make(chan []Vector, len(cm.nodes))
	errorChan := make(chan error, len(cm.nodes))
	var wg sync.WaitGroup

	for _, node := range cm.nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			vectors, err := n.searchNode(query, topN, filters)
			if err != nil {
				errorChan <- err
				return
			}
			resultsChan <- vectors
		}(node)
	}

	wg.Wait()
	close(resultsChan)
	close(errorChan)

	if len(errorChan) > 0 {
		return nil, <-errorChan
	}

	allResults := []Vector{}
	for results := range resultsChan {
		allResults = append(allResults, results...)
	}

	// Sort by similarity (assuming similarity score is in metadata)
	// TODO: Implement proper similarity ranking instead of placeholder sort
	return allResults, nil
}

// searchNode performs a search within a node by querying its shards
func (n *Node) searchNode(query []float64, topN int, filters map[string]string) ([]Vector, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	results := []Vector{}
	for _, shard := range n.shards {
		shard.mu.RLock()
		for _, v := range shard.vectors {
			if matchesFilters(v, filters) {
				results = append(results, v)
			}
		}
		shard.mu.RUnlock()
	}

	// Simulate ANN search - in production, this would involve an actual similarity metric like cosine similarity
	return results, nil
}

// matchesFilters checks if a vector's metadata matches the filters
func matchesFilters(v Vector, filters map[string]string) bool {
	for key, value := range filters {
		if v.Meta[key] != value {
			return false
		}
	}
	return true
}

// selectNode selects a node based on a hash of the vector ID (simple round-robin example)
func (cm *ClusterManager) selectNode(id string) *Node {
	hash := int(time.Now().UnixNano() % int64(len(cm.nodes)))
	return cm.nodes[hash]
}

// selectShard selects a shard within a node (simple selection for demo purposes)
func (n *Node) selectShard(id string) *Shard {
	for _, shard := range n.shards {
		return shard  // Simplified: In real systems, shards would be hashed and selected properly
	}
	return nil
}

func main() {
	// Initialize a cluster with 3 nodes
	cluster := NewClusterManager(3)

	// Add vectors to the cluster
	cluster.AddVector(Vector{ID: "vec1", Data: []float64{1.0, 2.0, 3.0}, Meta: map[string]string{"category": "image"}})
	cluster.AddVector(Vector{ID: "vec2", Data: []float64{4.0, 5.0, 6.0}, Meta: map[string]string{"category": "text"}})

	// Search vectors in the cluster
	query := []float64{1.0, 2.0, 3.5}
	filters := map[string]string{"category": "image"}
	results, err := cluster.SearchVectors(query, 2, filters)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Display results
	fmt.Println("Search results:")
	for _, v := range results {
		fmt.Printf("ID: %s, Data: %v, Meta: %v\n", v.ID, v.Data, v.Meta)
	}
}
