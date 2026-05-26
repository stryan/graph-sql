package graphsql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dominikbraun/graph"

	sq "github.com/Masterminds/squirrel"
)

// Store is a graph.Store implementation that uses an SQL database to store and retrieve graphs.
type Store[K comparable, T any] struct {
	db       *sql.DB
	config   Config
	registry map[string]*sql.Stmt
}

// New creates a new SQL store that can be passed to graph.NewWithStore. It expects a database
// connection directly to the actual database schema in the form of a sql.DB instance.
func New[K comparable, T any](db *sql.DB, config Config) (*Store[K, T], error) {
	registry := make(map[string]*sql.Stmt)
	return &Store[K, T]{
		db:       db,
		config:   config,
		registry: registry,
	}, nil
}

func (s *Store[K, T]) Close() error {
	var finalErr error
	for _, v := range s.registry {
		err := v.Close()
		if err != nil {
			finalErr = errors.Join(finalErr, err)
		}
	}
	return finalErr
}

// SetupTables creates all required tables inside the configured database. The schema is documented
// in this library's README file.
func (s *Store[K, T]) SetupTables() error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("unable to begin setup transaction :%w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(createVerticesTableSQL(s.config))
	if err != nil {
		return fmt.Errorf("failed to set up %s table: %w", s.config.VerticesTable, err)
	}
	_, err = tx.Exec(fmt.Sprintf("CREATE UNIQUE INDEX unq_vertex_hash ON %v(hash)", s.config.VerticesTable))
	if err != nil {
		return fmt.Errorf("error setting up unique index on vertice table: %w", err)
	}

	_, err = tx.Exec(createEdgesTableSQL(s.config))
	if err != nil {
		return fmt.Errorf("failed to set up %s table: %w", s.config.EdgesTable, err)
	}
	sql := fmt.Sprintf("CREATE UNIQUE INDEX unq_edge_hashes ON %v(source_hash,target_hash)", s.config.EdgesTable)
	_, err = tx.Exec(sql)
	if err != nil {
		return fmt.Errorf("error setting up unique index on edge table: %w", err)
	}
	// most of our lookups are on single nodes, so create an index for that too
	sql = fmt.Sprintf("CREATE INDEX idx_edge_target ON %v(target_hash)", s.config.EdgesTable)
	_, err = tx.Exec(sql)
	if err != nil {
		return fmt.Errorf("unable to setup index on target hash: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	vs, err := s.db.Prepare(fmt.Sprintf("SELECT value,weight,attributes FROM %s WHERE hash = ?",
		s.config.VerticesTable))
	if err != nil {
		return err
	}
	avs, err := s.db.Prepare(fmt.Sprintf("INSERT INTO %s (hash,value,weight,attributes) VALUES (?,?,?,?)",
		s.config.VerticesTable))
	if err != nil {
		return err
	}
	ed, err := s.db.Prepare(fmt.Sprintf("SELECT weight,attributes,data FROM %s WHERE source_hash = ? AND target_hash = ?", s.config.EdgesTable))
	if err != nil {
		return err
	}
	aed, err := s.db.Prepare(fmt.Sprintf("INSERT INTO %s (source_hash,target_hash,weight,attributes,data) VALUES (?,?,?,?,?)", s.config.EdgesTable))
	if err != nil {
		return err
	}
	s.registry["Vertex"] = vs
	s.registry["AddVertex"] = avs
	s.registry["Edge"] = ed
	s.registry["AddEdge"] = aed

	return nil
}

// DestroyTables drops all tables and thus removes all data from the database.
func (s *Store[K, T]) DestroyTables() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(dropEdgesTableSQL(s.config))
	if err != nil {
		return fmt.Errorf("failed to drop %s table: %w", s.config.EdgesTable, err)
	}

	_, err = tx.Exec(dropVerticesTableSQL(s.config))
	if err != nil {
		return fmt.Errorf("failed to drop %s table: %w", s.config.VerticesTable, err)
	}
	// reset the registry too since prepared statements are attached to the tabless
	s.registry = make(map[string]*sql.Stmt)
	return tx.Commit()
}

// AddVertex implements graph.Store.AddVertex.
func (s *Store[K, T]) AddVertex(hash K, value T, properties graph.VertexProperties) error {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	attributeBytes, err := json.Marshal(properties.Attributes)
	if err != nil {
		return err
	}
	stmt, ok := s.registry["AddVertex"]
	if !ok {
		return errors.New("no AddVertex statement")
	}
	_, err = stmt.Exec(hash, valueBytes, properties.Weight, attributeBytes)
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return graph.ErrVertexAlreadyExists
	} else if err != nil {
		return err
	}

	return err
}

// Vertex implements graph.Store.Vertex.
func (s *Store[K, T]) Vertex(hash K) (T, graph.VertexProperties, error) {
	var (
		valueBytes      []byte
		attributesBytes []byte
		value           T
		properties      graph.VertexProperties
	)
	stmt, ok := s.registry["Vertex"]
	if !ok {
		return value, properties, errors.New("no prepared vertex statement")
	}
	err := stmt.QueryRow(hash).Scan(&valueBytes, &properties.Weight, &attributesBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return value, properties, graph.ErrVertexNotFound
		}
		return value, properties, fmt.Errorf("failed to query vertex: %w", err)
	}

	if err = json.Unmarshal(valueBytes, &value); err != nil {
		return value, properties, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	if err = json.Unmarshal(attributesBytes, &properties.Attributes); err != nil {
		return value, properties, fmt.Errorf("failed to unmarshal attributes: %w", err)
	}

	return value, properties, nil
}

// ListVertices implements graph.Store.ListVertices.
func (s *Store[K, T]) ListVertices() ([]K, error) {
	rows, err := sq.
		Select("hash").
		From(s.config.VerticesTable).
		OrderBy("hash").
		RunWith(s.db).
		Query()
	if err != nil {
		return nil, fmt.Errorf("failed to query vertices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var hashes []K

	for rows.Next() {
		var hash K
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		hashes = append(hashes, hash)
	}

	return hashes, rows.Err()
}

// VertexCount implements graph.Store.VertexCount.
func (s *Store[K, T]) VertexCount() (int, error) {
	var count int

	err := sq.
		Select("count(hash)").
		From(s.config.VerticesTable).
		RunWith(s.db).
		QueryRow().
		Scan(&count)

	return count, err
}

// AddEdge implements graph.Store.AddEdge.
func (s *Store[K, T]) AddEdge(sourceHash, targetHash K, edge graph.Edge[K]) error {
	attributesBytes, err := json.Marshal(edge.Properties.Attributes)
	if err != nil {
		return err
	}
	var vertcount int
	err = sq.
		Select("count(hash)").
		From(s.config.VerticesTable).
		Where(sq.Or{
			sq.Eq{"hash": sourceHash},
			sq.Eq{"hash": targetHash},
		}).
		RunWith(s.db).
		Scan(&vertcount)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	expectedVerts := 2
	if sourceHash == targetHash {
		expectedVerts = 1
	}
	if vertcount != expectedVerts || errors.Is(err, sql.ErrNoRows) {
		return graph.ErrVertexNotFound
	}
	stmt, ok := s.registry["AddEdge"]
	if !ok {
		return errors.New("no prepared add edge statement")
	}
	_, err = stmt.Exec(sourceHash, targetHash, edge.Properties.Weight, attributesBytes, edge.Properties.Data)
	if err != nil && strings.Contains(err.Error(), "UNIQUE") {
		return graph.ErrEdgeAlreadyExists
	}
	return err
}

// RemoveEdge implements graph.Store.RemoveEdge.
func (s *Store[K, T]) RemoveEdge(sourceHash, targetHash K) error {
	var vertcount int
	err := sq.
		Select("count(hash)").
		From(s.config.VerticesTable).
		Where(sq.Or{
			sq.Eq{"hash": sourceHash},
			sq.Eq{"hash": targetHash},
		}).
		RunWith(s.db).
		Scan(&vertcount)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	expectedVerts := 2
	if sourceHash == targetHash {
		expectedVerts = 1
	}
	if vertcount != expectedVerts || errors.Is(err, sql.ErrNoRows) {
		return graph.ErrVertexNotFound
	}

	result, err := sq.
		Delete(s.config.EdgesTable).
		Where(sq.Eq{
			"source_hash": sourceHash,
			"target_hash": targetHash,
		}).
		RunWith(s.db).
		Exec()
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return graph.ErrEdgeNotFound
	}
	return nil
}

// Edge implements graph.Store.Edge.
func (s *Store[K, T]) Edge(sourceHash, targetHash K) (graph.Edge[K], error) {
	edge := graph.Edge[K]{
		Source: sourceHash,
		Target: targetHash,
	}
	stmt, ok := s.registry["Edge"]
	if !ok {
		return edge, errors.New("no prepared edge statement")
	}
	var attributesBytes []byte
	err := stmt.QueryRow(sourceHash, targetHash).
		Scan(&edge.Properties.Weight, &attributesBytes, &edge.Properties.Data)

	if errors.Is(err, sql.ErrNoRows) {
		return edge, graph.ErrEdgeNotFound
	}
	if err != nil {
		return edge, fmt.Errorf("failed to scan row: %w", err)
	}
	if err = json.Unmarshal(attributesBytes, &edge.Properties.Attributes); err != nil {
		return edge, fmt.Errorf("failed to unmarshal attributes: %w", err)
	}

	return edge, nil
}

// ListEdges implements graph.Store.ListEdges.
func (s *Store[K, T]) ListEdges() ([]graph.Edge[K], error) {
	rows, err := sq.
		Select(
			"source_hash",
			"target_hash",
			"weight",
			"attributes",
			"data",
		).
		From(s.config.EdgesTable).
		RunWith(s.db).
		Query()
	if err != nil {
		return nil, fmt.Errorf("failed to query edges: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var edges []graph.Edge[K]

	for rows.Next() {
		var (
			edge            graph.Edge[K]
			attributesBytes []byte
		)

		if err := rows.Scan(
			&edge.Source,
			&edge.Target,
			&edge.Properties.Weight,
			&attributesBytes,
			&edge.Properties.Data,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := json.Unmarshal(attributesBytes, &edge.Properties.Attributes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal attributes: %w", err)
		}

		edges = append(edges, edge)
	}

	return edges, rows.Err()
}

// RemoveVertex implements graph.Store.RemoveVertex.
// from https://github.com/dominikbraun/graph-sql/pull/3/files
func (s *Store[K, T]) RemoveVertex(hash K) error {
	// verify vertex exists
	count := 0
	err := sq.
		Select("count(hash)").
		From(s.config.VerticesTable).
		Where(sq.Eq{"hash": hash}).
		RunWith(s.db).
		QueryRow().
		Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return graph.ErrVertexNotFound
	}

	// check for edges
	edges := 0
	err = sq.
		Select("count(source_hash)").
		From(s.config.EdgesTable).
		Where(sq.Or{
			sq.Eq{"source_hash": hash},
			sq.Eq{"target_hash": hash},
		},
		).
		RunWith(s.db).
		QueryRow().
		Scan(&edges)
	if err != nil {
		return err
	}
	if edges != 0 {
		return graph.ErrVertexHasEdges
	}

	_, err = sq.
		Delete(s.config.VerticesTable).
		Where(sq.Eq{
			"hash": hash,
		}).
		RunWith(s.db).
		Exec()

	return err
}

// EdgeCount implements graph.Store.EdgeCount.
func (s *Store[K, T]) EdgeCount() (int, error) {
	var count int

	// Please note that for some reason count(id) does not return the correct results for sqlite.
	err := sq.
		Select("count(source_hash)").
		From(s.config.EdgesTable).
		RunWith(s.db).
		QueryRow().
		Scan(&count)

	return count, err
}

func (s *Store[K, T]) UpdateEdge(sourceHash, targetHash K, edge graph.Edge[K]) error {
	attributesBytes, err := json.Marshal(edge.Properties.Attributes)
	if err != nil {
		return err
	}

	modified, err := sq.Update(s.config.EdgesTable).
		Set("weight", edge.Properties.Weight).
		Set("attributes", attributesBytes).
		Set("data", edge.Properties.Data).
		Where("source_hash = ?", sourceHash).
		Where("target_hash = ?", targetHash).
		RunWith(s.db).
		Exec()
	if err != nil {
		return err
	}
	if rows, _ := modified.RowsAffected(); rows == 0 {
		return graph.ErrEdgeNotFound
	}
	return nil
}
