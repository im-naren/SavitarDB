package main

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// Vector represents a vector with an ID and data points
type Vector struct {
	ID   string
	Data []float64
	Meta map[string]string
}

// VectorDB is a distributed and scalable vector database
// Supports sharding and optimized concurrency
type VectorDB struct {
	shards []*Shard
	shardCount int
}

// Shard represents a partition of the database
type Shard struct {
	vectors map[string]Vector
	mu      sync.RWMutex
}

// NewVectorDB initializes a new vector database with sharding
func NewVectorDB(shardCount int) *VectorDB {
	shards := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &Shard{vectors: make(map[string]Vector)}
	}
	return &VectorDB{shards: shards, shardCount: shardCount}
}

// hashID determines the shard based on the vector ID
func (db *VectorDB) hashID(id string) int {
	hash := 0
	for _, char := range id {
		hash = (hash*31 + int(char)) % db.shardCount
	}
	return hash
}

// AddVector adds a new vector to the appropriate shard
func (db *VectorDB) AddVector(v Vector) error {
	shard := db.shards[db.hashID(v.ID)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.vectors[v.ID]; exists {
		return errors.New("vector with this ID already exists")
	}
	shard.vectors[v.ID] = v
	return nil
}

// UpdateVector updates an existing vector in the appropriate shard
func (db *VectorDB) UpdateVector(v Vector) error {
	shard := db.shards[db.hashID(v.ID)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.vectors[v.ID]; !exists {
		return errors.New("vector not found")
	}
	shard.vectors[v.ID] = v
	return nil
}

// GetVector retrieves a vector by its ID from the appropriate shard
func (db *VectorDB) GetVector(id string) (Vector, error) {
	shard := db.shards[db.hashID(id)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	v, exists := shard.vectors[id]
	if !exists {
		return Vector{}, errors.New("vector not found")
	}
	return v, nil
}

// DeleteVector removes a vector by its ID from the appropriate shard
func (db *VectorDB) DeleteVector(id string) error {
	shard := db.shards[db.hashID(id)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.vectors[id]; !exists {
		return errors.New("vector not found")
	}
	delete(shard.vectors, id)
	return nil
}

// CosineSimilarity calculates the cosine similarity between two vectors
func CosineSimilarity(v1, v2 []float64) (float64, error) {
	if len(v1) != len(v2) {
		return 0, errors.New("vectors must have the same length")
	}

	var dotProduct, normA, normB float64
	for i := 0; i < len(v1); i++ {
		dotProduct += v1[i] * v2[i]
		normA += v1[i] * v1[i]
		normB += v2[i] * v2[i]
	}

	if normA == 0 || normB == 0 {
		return 0, errors.New("zero vector detected")
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB)), nil
}

// SearchVectors finds the top N most similar vectors with optional filtering and load balancing across shards
func (db *VectorDB) SearchVectors(query []float64, topN int, filters map[string]string) ([]Vector, error) {
	resultsChan := make(chan []Vector, db.shardCount)
	errorChan := make(chan error, db.shardCount)
	var wg sync.WaitGroup

	for _, shard := range db.shards {
		wg.Add(1)
		go func(s *Shard) {
			defer wg.Done()
			vectors, err := s.searchShard(query, topN, filters)
			if err != nil {
				errorChan <- err
				return
			}
			resultsChan <- vectors
		}(shard)
	}

	wg.Wait()
	close(resultsChan)
	close(errorChan)

	if len(errorChan) > 0 {
		return nil, <-errorChan
	}

	// Combine results from all shards and sort
	allResults := []Vector{}
	for results := range resultsChan {
		allResults = append(allResults, results...)
	}

	sort.Slice(allResults, func(i, j int) bool {
		// Assume a pre-computed similarity score is part of metadata
		return allResults[i].Meta["similarity"] > allResults[j].Meta["similarity"]
	})

	if len(allResults) > topN {
		allResults = allResults[:topN]
	}

	return allResults, nil
}

// searchShard performs a search on a single shard
func (s *Shard) searchShard(query []float64, topN int, filters map[string]string) ([]Vector, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type result struct {
		vector     Vector
		similarity float64
	}

	var results []result
	for _, v := range s.vectors {
		if matchesFilters(v, filters) {
			sim, err := CosineSimilarity(query, v.Data)
			if err != nil {
				return nil, err
			}
			v.Meta["similarity"] = fmt.Sprintf("%f", sim)
			results = append(results, result{vector: v, similarity: sim})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].similarity > results[j].similarity
	})

	var topResults []Vector
	for i := 0; i < topN && i < len(results); i++ {
		topResults = append(topResults, results[i].vector)
	}

	return topResults, nil
}

// matchesFilters checks if a vector's metadata matches the given filters
func matchesFilters(v Vector, filters map[string]string) bool {
	for key, value := range filters {
		if v.Meta[key] != value {
			return false
		}
	}
	return true
}

func main() {
	db := NewVectorDB(4)  // Initialize with 4 shards for scalability

	// Add sample vectors
	db.AddVector(Vector{ID: "vec1", Data: []float64{1.0, 2.0, 3.0}, Meta: map[string]string{"category": "image"}})
	db.AddVector(Vector{ID: "vec2", Data: []float64{4.0, 5.0, 6.0}, Meta: map[string]string{"category": "text"}})
	db.AddVector(Vector{ID: "vec3", Data: []float64{7.0, 8.0, 9.0}, Meta: map[string]string{"category": "image"}})

	// Search across shards
	query := []float64{1.0, 2.0, 3.5}
	filters := map[string]string{"category": "image"}
	results, err := db.SearchVectors(query, 2, filters)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Display results
	fmt.Println("Top results:")
	for _, v := range results {
		fmt.Printf("ID: %s, Data: %v, Meta: %v\n", v.ID, v.Data, v.Meta)
	}
}
