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
	Meta map[string]string  // Metadata associated with the vector
}

// VectorDB is a vector database with in-memory storage and concurrency support
type VectorDB struct {
	vectors map[string]Vector
	mu      sync.RWMutex
}

// NewVectorDB initializes a new vector database
func NewVectorDB() *VectorDB {
	return &VectorDB{
		vectors: make(map[string]Vector),
	}
}

// AddVector adds a new vector to the database
func (db *VectorDB) AddVector(v Vector) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.vectors[v.ID]; exists {
		return errors.New("vector with this ID already exists")
	}
	db.vectors[v.ID] = v
	return nil
}

// UpdateVector updates an existing vector in the database
func (db *VectorDB) UpdateVector(v Vector) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.vectors[v.ID]; !exists {
		return errors.New("vector not found")
	}
	db.vectors[v.ID] = v
	return nil
}

// GetVector retrieves a vector by its ID
func (db *VectorDB) GetVector(id string) (Vector, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	v, exists := db.vectors[id]
	if !exists {
		return Vector{}, errors.New("vector not found")
	}
	return v, nil
}

// DeleteVector removes a vector by its ID
func (db *VectorDB) DeleteVector(id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.vectors[id]; !exists {
		return errors.New("vector not found")
	}
	delete(db.vectors, id)
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

// SearchVectors finds the top N most similar vectors to the query vector with optional metadata filtering
func (db *VectorDB) SearchVectors(query []float64, topN int, filters map[string]string) ([]Vector, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	type result struct {
		vector     Vector
		similarity float64
	}

	var results []result
	for _, v := range db.vectors {
		if matchesFilters(v, filters) {
			sim, err := CosineSimilarity(query, v.Data)
			if err != nil {
				return nil, err
			}
			results = append(results, result{vector: v, similarity: sim})
		}
	}

	// Sort results by similarity in descending order
	sort.Slice(results, func(i, j int) bool {
		return results[i].similarity > results[j].similarity
	})

	// Return the top N results
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
	// Initialize the database
	db := NewVectorDB()

	// Add some sample vectors with metadata
	_ = db.AddVector(Vector{ID: "vec1", Data: []float64{1.0, 2.0, 3.0}, Meta: map[string]string{"category": "image"}})
	_ = db.AddVector(Vector{ID: "vec2", Data: []float64{4.0, 5.0, 6.0}, Meta: map[string]string{"category": "text"}})
	_ = db.AddVector(Vector{ID: "vec3", Data: []float64{7.0, 8.0, 9.0}, Meta: map[string]string{"category": "image"}})

	// Search for similar vectors with a filter
	queryVector := []float64{1.0, 2.0, 3.5}
	filters := map[string]string{"category": "image"}
	results, err := db.SearchVectors(queryVector, 2, filters)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Display search results
	fmt.Println("Top similar vectors with filters:")
	for _, v := range results {
		fmt.Printf("ID: %s, Data: %v, Meta: %v\n", v.ID, v.Data, v.Meta)
	}
}
