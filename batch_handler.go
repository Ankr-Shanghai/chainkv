package main

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
)

func NewBatchHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	rsp := &pb.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewBatch(kvs)
	return rsp
}

func BatchPutHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchPut(kvs, req.Id, req.Key, req.Val)
	return rsp
}

func BatchDelHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchDel(kvs, req.Id, req.Key)
	return rsp
}

func BatchWriteHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)
	err = BatchWrite(kvs, req.Id)
	if err != nil {
		kvs.log.Error("BatchWriteHandler", "err", err)
		rsp.Code = retcode.ErrBatchWrite
	}
	return rsp
}

func BatchResetHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	BatchReset(kvs, req.Id)
	return rsp
}

func BatchCloseHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)

	BatchClose(kvs, req.Id)

	return rsp
}
