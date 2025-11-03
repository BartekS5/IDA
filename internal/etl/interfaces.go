package etl

// Extractor reads data in batches from a source.
// ReadBatch fetches the next batch of data. `batchSize` is the desired
// number of records. `checkpoint` is the value (e.g., last processed ID)
// from which to resume reading.
// It returns a slice of data records (as interface{} for now) and an error.
type Extractor interface {
	ReadBatch(batchSize int, checkpoint string) ([]interface{}, error)
}

// Transformer converts data based on the mapping rules.
// Transform takes a batch of raw data from the Extractor and returns
// a batch of data ready for the Loader.
type Transformer interface {
	Transform(batch []interface{}) ([]interface{}, error)
}

// Loader writes data in batches to a target.
// LoadBatch takes a batch of transformed data and writes it to the
// destination datastore.
type Loader interface {
	LoadBatch(batch []interface{}) error
}
