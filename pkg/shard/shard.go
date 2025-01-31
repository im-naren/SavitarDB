package shard

import (
	"errors"
	"sync"

	"github.com/im-naren/savitar/pkg/vector"
)

// Shard stores a partition of the vector data and supports concurrent operations
type Shard struct {
	vectors map[string]vector.Vector
	mu      sync.RWMutex
}

// NewShard initializes a new shard instance
func NewShard() *Shard {
	return &Shard{
		vectors: make(map[string]vector.Vector),
	}
}

// AddVector adds a vector to the shard
func (s *Shard) AddVector(v vector.Vector) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vectors[v.ID]; exists {
		return errors.New("vector with this ID already exists")
	}
	s.vectors[v.ID] = v
	return nil
}

// GetVector retrieves a vector by its ID
func (s *Shard) GetVector(id string) (vector.Vector, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, exists := s.vectors[id]
	if !exists {
		return vector.Vector{}, errors.New("vector not found")
	}
	return v, nil
}

// DeleteVector removes a vector by ID
func (s *Shard) DeleteVector(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.vectors[id]; !exists {
		return errors.New("vector not found")
	}
	delete(s.vectors, id)
	return nil
}
