package main

import (
	"encoding/binary"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func GetSequenceHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	kvs.seqLock.Lock()
	defer kvs.seqLock.Unlock()

	rsp.Val = kvs.buffer.GetLen(8)
	defer kvs.buffer.Put(rsp.Val)
	binary.BigEndian.PutUint64(rsp.Val, kvs.sequence)

	kvs.sequence++

	return rsp
}
