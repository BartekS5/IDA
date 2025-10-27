// Package models defines the public-facing data structures for the application.
// These structs are used to parse the mapping.json configuration file
// and can be imported by other projects that need to interact with this tool.
package models

import "encoding/json"

// MappingConfig is the top-level structure for the mapping.json file.
// It matches the root of your JSON design.
type MappingConfig struct {
	Version        float64         `json:"version"`
	GlobalSettings GlobalSettings  `json:"global_settings"`
	MigrationTasks []MigrationTask `json:"migration_tasks"`
}

// GlobalSettings defines default behaviors for the migration.
type GlobalSettings struct {
	OnSQLNull      string `json:"on_sql_null"`      // "omit_field", "set_null", "use_default"
	OnMongoMissing string `json:"on_mongo_missing"` // "set_to_sql_null", "skip_field", "use_default"
}

// MigrationTask defines a single, complete migration from one
// table to one collection (or vice-versa).
type MigrationTask struct {
	Name          string         `json:"name"`
	SQLEntity     SQLEntity      `json:"sql_entity"`
	MongoEntity   MongoEntity    `json:"mongo_entity"`
	FieldMappings []FieldMapping `json:"field_mappings"`
}

// SQLEntity defines the source SQL table details.
type SQLEntity struct {
	TableName  string `json:"table_name"`
	PrimaryKey string `json:"primary_key"`
}

// MongoEntity defines the target Mongo collection details.
type MongoEntity struct {
	CollectionName string `json:"collection_name"`
	UpsertKey      string `json:"upsert_key"`
}

// FieldMapping defines a single column-to-field mapping,
// including type conversion rules.
type FieldMapping struct {
	SQLColumn         string          `json:"sql_column"`
	MongoField        string          `json:"mongo_field"`
	TypeMapping       string          `json:"type_mapping"`
	SQLNotNullDefault json.RawMessage `json:"sql_not_null_default,omitempty"`
}
