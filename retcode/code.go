package retcode

const (
	CodeOK = 6000 + iota
	ErrCodeUnmarshal
	ErrCodeNotSupport
	ErrCodePut
	ErrCodeGet
	ErrCodeNotFound
)
