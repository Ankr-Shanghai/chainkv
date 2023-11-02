package main

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
)

func NewIteratorHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	rsp := &pb.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewIter(kvs, req.Key, req.Val)
	return rsp
}

func IterNextHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterNext(kvs, req.Id)
	return rsp
}

func IterKeyHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterKey(kvs, req.Id)
	return rsp
}

func IterValHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterValue(kvs, req.Id)
	return rsp
}

func IterErrorHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterError(kvs, req.Id) != nil
	return rsp
}

func IterCloseHandler(kvs *kvserver, req *pb.Request) *pb.Response {
	var (
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
	)
	IterClose(kvs, req.Id)
	return rsp
}
