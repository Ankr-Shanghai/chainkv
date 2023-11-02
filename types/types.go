package types

type Request struct {
	Type ReqType
	Key  []byte
	Val  []byte
	Id   uint32
}

type Response struct {
	Code  int32
	Val   []byte
	Id    uint32
	Exist bool
}
