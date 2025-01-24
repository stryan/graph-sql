package graphsql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dominikbraun/graph"

	_ "github.com/mattn/go-sqlite3"
)

func createStore[K comparable, T any](c Config) (*Store[K, T], error) {
	db, err := sql.Open("sqlite3", "file::memory:")
	if err != nil {
		panic(err)
	}

	store := New[K, T](db, c)
	if store == nil {
		return nil, fmt.Errorf("failed to create new store")
	}

	if err := store.SetupTables(); err != nil {
		return nil, err
	}

	return store, nil
}

func TestImplementsStoreInterface(t *testing.T) {
	store := Store[int, int]{}

	// this will throw a compile error if graphsql.Store doesn't implement the graph.Store interface
	var _ graph.Store[int, int] = (*Store[int, int])(&store)
}

func TestUnique(t *testing.T) {
	assert := assert.New(t)
	conf := DefaultConfig
	conf.Unique = true
	store, err := createStore[int, int](conf)
	assert.Nil(err)
	assert.NotNil(store)

	assert.Nil(store.AddVertex(1, 1, graph.VertexProperties{}))
	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Error(err)
	assert.Equal(graph.ErrVertexAlreadyExists, err)
}

func TestAddEdge(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	err = store.AddEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Equal(graph.ErrVertexNotFound, err)

	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(2, 2, graph.VertexProperties{})
	assert.Nil(err)

	err = store.AddEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Nil(err)
	err = store.AddEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Equal(graph.ErrEdgeAlreadyExists, err)
}

func TestEdgeCount(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(2, 2, graph.VertexProperties{})
	assert.Nil(err)

	edgeCount, err := store.EdgeCount()
	assert.Nil(err)
	assert.Equal(0, edgeCount)

	err = store.AddEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Nil(err)

	edgeCount, err = store.EdgeCount()
	assert.Nil(err)
	assert.Equal(1, edgeCount)

	err = store.AddEdge(2, 1, graph.Edge[int]{Source: 2, Target: 1, Properties: graph.EdgeProperties{}})
	assert.Nil(err)

	edgeCount, err = store.EdgeCount()
	assert.Nil(err)
	assert.Equal(2, edgeCount)

	err = store.AddEdge(1, 1, graph.Edge[int]{Source: 1, Target: 1, Properties: graph.EdgeProperties{}})
	assert.Nil(err)

	edgeCount, err = store.EdgeCount()
	assert.Nil(err)
	assert.Equal(3, edgeCount)

	err = store.AddEdge(2, 2, graph.Edge[int]{Source: 2, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Nil(err)

	edgeCount, err = store.EdgeCount()
	assert.Nil(err)
	assert.Equal(4, edgeCount)

	err = store.RemoveEdge(2, 2)
	assert.Nil(err)

	edgeCount, err = store.EdgeCount()
	assert.Nil(err)
	assert.Equal(3, edgeCount)
}

func TestAddVertex(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	_, _, err = store.Vertex(1)
	assert.Equal(graph.ErrVertexNotFound, err)
	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)
	_, _, err = store.Vertex(1)

	assert.Nil(err)
}

func TestVertexCount(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	count, err := store.VertexCount()
	assert.Nil(err)
	assert.Equal(0, count)
	vertexCount := 20
	for i := 0; i < vertexCount; i++ {
		err = store.AddVertex(i, i, graph.VertexProperties{})
		assert.Nil(err)
	}
	count, err = store.VertexCount()
	assert.Nil(err)
	assert.Equal(vertexCount, count)
	err = store.RemoveVertex(vertexCount - 1)
	assert.Nil(err)
	count, err = store.VertexCount()
	assert.Nil(err)
	assert.Equal(vertexCount-1, count)
}

func TestRemoveVertex(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)

	vertexCount, err := store.VertexCount()
	assert.Nil(err)
	assert.Equal(1, vertexCount)

	err = store.RemoveVertex(1)
	assert.Nil(err)

	vertexCount, err = store.VertexCount()
	assert.Nil(err)
	assert.Equal(0, vertexCount)

	// larger graph
	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(2, 2, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(3, 3, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(4, 4, graph.VertexProperties{})
	assert.Nil(err)

	vertexCount, err = store.VertexCount()
	assert.Nil(err)
	assert.Equal(4, vertexCount)

	err = store.RemoveVertex(3)
	assert.Nil(err)

	vertexCount, err = store.VertexCount()
	assert.Nil(err)
	assert.Equal(3, vertexCount)

	_, _, err = store.Vertex(3)
	assert.NotNil(err)
}

func TestUpdateEdge(t *testing.T) {
	assert := assert.New(t)

	store, err := createStore[int, int](DefaultConfig)
	assert.Nil(err)
	assert.NotNil(store)

	err = store.UpdateEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Equal(graph.ErrEdgeNotFound, err)

	err = store.AddVertex(1, 1, graph.VertexProperties{})
	assert.Nil(err)
	err = store.AddVertex(2, 2, graph.VertexProperties{})
	assert.Nil(err)

	err = store.AddEdge(1, 2, graph.Edge[int]{Source: 1, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Nil(err)
	err = store.AddEdge(2, 1, graph.Edge[int]{Source: 2, Target: 1, Properties: graph.EdgeProperties{}})
	assert.Nil(err)
	err = store.AddEdge(1, 1, graph.Edge[int]{Source: 1, Target: 1, Properties: graph.EdgeProperties{}})
	assert.Nil(err)
	err = store.AddEdge(2, 2, graph.Edge[int]{Source: 2, Target: 2, Properties: graph.EdgeProperties{}})
	assert.Nil(err)

	err = store.UpdateEdge(1, 1, graph.Edge[int]{Source: 1, Target: 1, Properties: graph.EdgeProperties{
		Attributes: map[string]string{"abc": "xyz"},
		Weight:     5,
		Data:       "happy",
	}})

	assert.Nil(err)

	edge, err := store.Edge(1, 1)
	assert.Nil(err)
	assert.Equal(5, edge.Properties.Weight)
	assert.Equal("xyz", edge.Properties.Attributes["abc"])
	assert.Equal("happy", edge.Properties.Data)
}
