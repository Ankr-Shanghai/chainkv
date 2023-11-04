package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestMarshalAndUnmarshal(t *testing.T) {

	req := &Request{
		Type: REQ_TYPE_PUT,
		Key:  []byte("key"),
		Val:  []byte("val"),
		Id:   1,
	}

	data := req.Marshal()

	rs := &Request{}

	rs.Unmarshal(data)

	assert.Equal(t, req.Type, rs.Type)
	assert.Equal(t, req.Id, rs.Id)
	assert.Equal(t, req.Key, rs.Key)
	assert.Equal(t, req.Val, rs.Val)

}

func TestResponseMarshalAndUnmarshal(t *testing.T) {

	rsp := &Response{
		Code:  200,
		Val:   []byte("val"),
		Id:    1,
		Exist: true,
	}

	data := rsp.Marshal()

	rs := &Response{}

	rs.Unmarshal(data)

	assert.Equal(t, rsp.Code, rs.Code)
	assert.Equal(t, rsp.Id, rs.Id)
	assert.Equal(t, rsp.Val, rs.Val)
	assert.Equal(t, rsp.Exist, rs.Exist)

}
