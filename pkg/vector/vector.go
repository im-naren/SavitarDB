package vector

import (
	"errors"
	"math"
)

// Vector represents a vector with an ID, data points, and metadata
type Vector struct {
	ID   string
	Data []float64
	Meta map[string]string
}

// Validate checks if the vector data is valid (non-empty and finite values)
func (v *Vector) Validate() error {
	if len(v.Data) == 0 {
		return errors.New("vector data cannot be empty")
	}
	for _, val := range v.Data {
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return errors.New("vector contains invalid values")
		}
	}
	return nil
}

// CosineSimilarity calculates the cosine similarity between two vectors
func CosineSimilarity(v1, v2 *Vector) (float64, error) {
	if len(v1.Data) != len(v2.Data) {
		return 0, errors.New("vectors must have the same length")
	}

	var dotProduct, normA, normB float64
	for i := 0; i < len(v1.Data); i++ {
		dotProduct += v1.Data[i] * v2.Data[i]
		normA += v1.Data[i] * v1.Data[i]
		normB += v2.Data[i] * v2.Data[i]
	}

	if normA == 0 || normB == 0 {
		return 0, errors.New("zero vector detected")
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB)), nil
}
