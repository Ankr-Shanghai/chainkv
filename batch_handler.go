package main

import (
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewBatchHandler(kvs *kvserver, req *types.Request) *types.Response {
	rsp := &types.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewBatch(kvs)
	return rsp
}

func BatchPutHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchPut(kvs, req.Id.String(), req.Key, req.Val)

	return rsp
}

func BatchDelHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchDel(kvs, req.Id.String(), req.Key)
	return rsp
}

func BatchWriteHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)
	err = BatchWrite(kvs, req.Id.String())
	if err != nil {
		kvs.log.Error("BatchWriteHandler", "err", err)
		rsp.Code = retcode.ErrBatchWrite
	}
	return rsp
}

func BatchResetHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchReset(kvs, req.Id.String())
	return rsp
}

func BatchCloseHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)

	BatchClose(kvs, req.Id.String())

	return rsp
}
