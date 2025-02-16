package graphsql

import "fmt"

const (
	createVerticesTable = `
CREATE TABLE %s (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    hash %s,
    value %s,
	weight INT,
	attributes JSON
);`
	safeCreateVerticesTable = `
CREATE TABLE IF NOT EXISTS %s (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    hash %s,
    value %s,
	weight INT,
	attributes JSON
);
	`
	createEdgesTable = `
CREATE TABLE %s (
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	source_hash %s,
	target_hash %s,
	weight INT,
	attributes JSON,
	data BLOB
);`
	safeCreateEdgesTable = `
CREATE TABLE IF NOT EXISTS %s (
	id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	source_hash %s,
	target_hash %s,
	weight INT,
	attributes JSON,
	data BLOB
);`
	dropTable     = `DROP TABLE %s;`
	safeDropTable = `DROP TABLE IF EXISTS %s;`
)

// DefaultConfig is a sane default configuration of the table schema. Using DefaultConfig when
// creating a store using New makes sense for most users.
var DefaultConfig = Config{
	VerticesTable:   "vertices",
	EdgesTable:      "edges",
	VertexHashType:  "TEXT",
	VertexValueType: "JSON",
	Safe:            false,
	Unique:          false,
}

var SafeConfig = Config{
	VerticesTable:   "vertices",
	EdgesTable:      "edges",
	VertexHashType:  "TEXT",
	VertexValueType: "JSON",
	Safe:            true,
	Unique:          true,
}

// Config configures the table schema, i.e. the table names and some data types of its columns.
type Config struct {
	VerticesTable   string
	EdgesTable      string
	VertexHashType  string
	VertexValueType string
	Safe            bool
	Unique          bool
}

func createVerticesTableSQL(c Config) string {
	createSQL := createVerticesTable
	if c.Safe {
		createSQL = safeCreateVerticesTable
	}
	return fmt.Sprintf(
		createSQL,
		c.VerticesTable,
		c.VertexHashType,
		c.VertexValueType,
	)
}

func createEdgesTableSQL(c Config) string {
	createSQL := createEdgesTable
	if c.Safe {
		createSQL = safeCreateEdgesTable
	}
	return fmt.Sprintf(
		createSQL,
		c.EdgesTable,
		c.VertexHashType,
		c.VertexHashType,
	)
}

func dropVerticesTableSQL(c Config) string {
	dropSql := dropTable
	if c.Safe {
		dropSql = safeDropTable
	}
	return fmt.Sprintf(
		dropSql,
		c.VerticesTable,
	)
}

func dropEdgesTableSQL(c Config) string {
	dropSql := dropTable
	if c.Safe {
		dropSql = safeDropTable
	}
	return fmt.Sprintf(
		dropSql,
		c.EdgesTable,
	)
}
