package retcode

const (
	CodeOK = 6000 + iota
	ErrUnmarshal
	ErrNotSupport
	ErrPut
	ErrGet
	ErrNotFound
	ErrBatchReset
	ErrBatchWrite
	ErrFlush
)
