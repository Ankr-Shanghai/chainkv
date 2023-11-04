package types

import (
	"encoding/binary"
	"errors"

	"github.com/gobwas/pool/pbytes"
)

type Request struct {
	Type ReqType
	Key  []byte
	Val  []byte
	Id   uint32
}

func (req *Request) Marshal() []byte {
	keyLen := len(req.Key)
	rs := make([]byte, 0, 12+keyLen+len(req.Val))
	buf := pbytes.GetLen(4)
	defer pbytes.Put(buf)
	binary.BigEndian.PutUint32(buf, uint32(req.Type))
	rs = append(rs, buf...)
	binary.BigEndian.PutUint32(buf, req.Id)
	rs = append(rs, buf...)
	binary.BigEndian.PutUint32(buf, uint32(keyLen))
	rs = append(rs, buf...)
	rs = append(rs, req.Key...)
	rs = append(rs, req.Val...)
	return rs
}

func (req *Request) Unmarshal(data []byte) error {
	if len(data) < 12 {
		return errors.New("invalid request data")
	}
	req.Type = ReqType(binary.BigEndian.Uint32(data[:4]))
	req.Id = binary.BigEndian.Uint32(data[4:8])
	keyLen := binary.BigEndian.Uint32(data[8:12])
	if len(data) < 12+int(keyLen) {
		return errors.New("invalid request data")
	}
	req.Key = make([]byte, keyLen)
	copy(req.Key, data[12:12+keyLen])
	req.Val = make([]byte, len(data[12+keyLen:]))
	copy(req.Val, data[12+keyLen:])

	return nil
}

type Response struct {
	Code  int32
	Val   []byte
	Id    uint32
	Exist bool
}

func (rsp *Response) Marshal() []byte {
	rs := make([]byte, 0, 9+len(rsp.Val))
	buf := pbytes.GetLen(4)
	defer pbytes.Put(buf)
	binary.BigEndian.PutUint32(buf, uint32(rsp.Code))
	rs = append(rs, buf...)
	binary.BigEndian.PutUint32(buf, rsp.Id)
	rs = append(rs, buf...)
	if rsp.Exist {
		rs = append(rs, 0x01)
	}
	rs = append(rs, rsp.Val...)
	return rs
}

func (rsp *Response) Unmarshal(data []byte) error {

	if len(data) < 9 {
		return errors.New("invalid response data")
	}

	rsp.Code = int32(binary.BigEndian.Uint32(data[:4]))
	rsp.Id = binary.BigEndian.Uint32(data[4:8])
	if data[8] == 0x01 {
		rsp.Exist = true
	}

	rsp.Val = make([]byte, len(data[9:]))
	copy(rsp.Val, data[9:])

	return nil
}
