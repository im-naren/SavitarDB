package main

import (
    "fmt"

    "github.com/im-naren/savitar/pkg/cluster"
    "github.com/im-naren/savitar/pkg/vector"
)

func main() {
    // Initialize a cluster with 3 nodes
    cm := cluster.NewClusterManager(3)

    // Add sample vectors
    vector1 := vector.Vector{ID: "vec1", Data: []float64{1.0, 2.0, 3.0}, Meta: map[string]string{"type": "image"}}
    vector2 := vector.Vector{ID: "vec2", Data: []float64{4.0, 5.0, 6.0}, Meta: map[string]string{"type": "text"}}

    // Add vectors to the cluster
    if err := cm.AddVector(vector1); err != nil {
        fmt.Println("Error adding vector1:", err)
        return
    }
    if err := cm.AddVector(vector2); err != nil {
        fmt.Println("Error adding vector2:", err)
        return
    }

    // Retrieve a vector from the cluster
    retrieved, err := cm.GetVector("vec1")
    if err != nil {
        fmt.Println("Error retrieving vector:", err)
        return
    }

    fmt.Printf("Retrieved vector: %+v\n", retrieved)
}