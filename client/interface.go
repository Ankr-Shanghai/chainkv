package client

type Client interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Close() error
	Has(key []byte) (bool, error)
	NewBatch() (*Batch, error)
	NewIter(start, end []byte) (*Iterator, error)
	NewSnap() (*Snap, error)
	Sequence() (uint64, error)
}
