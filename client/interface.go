package client

type Client interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Close() error
	Has(key []byte) (bool, error)
	NewBatch() *Batch
}
