package main

import (
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewIteratorHandler(kvs *kvserver, req *types.Request) *types.Response {
	rsp := &types.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewIter(kvs, req.Key, req.Val)
	return rsp
}

func IterNextHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterNext(kvs, req.Id)
	return rsp
}

func IterKeyHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterKey(kvs, req.Id)
	return rsp
}

func IterValHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterValue(kvs, req.Id)
	return rsp
}

func IterErrorHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterError(kvs, req.Id) != nil
	return rsp
}

func IterCloseHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	IterClose(kvs, req.Id)
	return rsp
}
