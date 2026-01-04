package etl

// Extractor reads data. Returns a slice of maps (generic representation), the next offset, and error.
type Extractor interface {
	Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error)
}

// Loader writes data.
type Loader interface {
	Load(data []map[string]interface{}) error
}
