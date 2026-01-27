package etl

type Extractor interface {
	Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error)
}

type Loader interface {
	Load(data []map[string]interface{}) error
}
