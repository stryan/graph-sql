package graphsql

import "fmt"

const (
	createVerticesTable = `
CREATE TABLE IF NOT EXISTS %s (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    hash %s,
    value %s,
	weight INT,
	attributes JSON
);
	`
	createEdgesTable = `
CREATE TABLE IF NOT EXISTS %s (
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	source_hash %s,
	target_hash %s,
	weight INT,
	attributes JSON,
	data BLOB
);`
	dropTable = `DROP TABLE IF EXISTS %s;`
)

// DefaultConfig is a sane default configuration of the table schema. Using DefaultConfig when
// creating a store using New makes sense for most users.
var DefaultConfig = Config{
	VerticesTable:   "vertices",
	EdgesTable:      "edges",
	VertexHashType:  "TEXT",
	VertexValueType: "JSON",
	Unique:          false,
}

var SafeConfig = Config{
	VerticesTable:   "vertices",
	EdgesTable:      "edges",
	VertexHashType:  "TEXT",
	VertexValueType: "JSON",
	Unique:          true,
}

// Config configures the table schema, i.e. the table names and some data types of its columns.
type Config struct {
	VerticesTable   string
	EdgesTable      string
	VertexHashType  string
	VertexValueType string
	Unique          bool
}

func createVerticesTableSQL(c Config) string {
	return fmt.Sprintf(
		createVerticesTable,
		c.VerticesTable,
		c.VertexHashType,
		c.VertexValueType,
	)
}

func createEdgesTableSQL(c Config) string {
	return fmt.Sprintf(
		createEdgesTable,
		c.EdgesTable,
		c.VertexHashType,
		c.VertexHashType,
	)
}

func dropVerticesTableSQL(c Config) string {
	return fmt.Sprintf(
		dropTable,
		c.VerticesTable,
	)
}

func dropEdgesTableSQL(c Config) string {
	return fmt.Sprintf(
		dropTable,
		c.EdgesTable,
	)
}
