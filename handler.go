package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/cockroachdb/pebble"
)

func FlushDBHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)

	err := kv.db.Flush()
	if err != nil {
		rsp.Code = retcode.ErrFlush
	}

	return rsp
}

func PutHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	err = kv.db.Set(req.Key, req.Val, kv.wo)
	if err != nil {
		kv.log.Error("PutHandler", "err", err)
		rsp.Code = retcode.ErrPut
	}

	return rsp
}

func GetHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	rsp.Val, closer, err = kv.db.Get(req.Key)
	if err != nil {
		if err == pebble.ErrNotFound {
			rsp.Code = retcode.ErrNotFound
		} else {
			rsp.Code = retcode.ErrGet
		}
	}

	if closer != nil {
		closer.Close()
	}

	return rsp
}

func DelHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	err = kv.db.Delete(req.Key, nil)
	if err != nil {
		kv.log.Error("DelHandler", "err", err)
		rsp.Code = retcode.ErrGet
	}

	return rsp
}

func HasHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	_, closer, err = kv.db.Get(req.Key)
	if err != nil {
		rsp.Exist = false
		return rsp
	}

	if closer != nil {
		closer.Close()
	}
	rsp.Exist = true

	return rsp
}
