package models

import "encoding/json"

// MappingSchema represents the root of the JSON mapping file.
type MappingSchema struct {
	Entity          string                     `json:"entity"`
	SQLTable        string                     `json:"sqlTable"`
	MongoCollection string                     `json:"mongoCollection"`
	IDStrategy      IDStrategy                 `json:"idStrategy"`
	Fields          map[string]FieldConfig     `json:"fields"`
	Relations       map[string]RelationConfig  `json:"relations"`
}

type IDStrategy struct {
	SQLField   string `json:"sqlField"`
	MongoField string `json:"mongoField"`
	Type       string `json:"type"`
}

type FieldConfig struct {
	SQLColumn  string `json:"sql"`
	MongoField string `json:"mongo"`
	Type       string `json:"type"`
	Format     string `json:"format,omitempty"`
}

type RelationConfig struct {
	Type          string   `json:"type"`
	SQLTable      string   `json:"sqlTable,omitempty"`
	SQLJoinTable  string   `json:"sqlJoinTable,omitempty"` // Fixed: Added 'string' type
	SQLForeignKey string   `json:"sqlForeignKey"`
	MongoField    string   `json:"mongoField"`
	Embedding     string   `json:"embedding"`
	Fields        []string `json:"fields,omitempty"`
	ReferenceKey  string   `json:"referenceKey,omitempty"`
}

func LoadMapping(data []byte) (*MappingSchema, error) {
	var m MappingSchema
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
