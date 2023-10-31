package client

import "errors"

var (
	ErrNotFound = errors.New("not found")
	ErrNewBatch = errors.New("new batch failed")
)
