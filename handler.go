package main

import (
	"crypto/md5"
	"fmt"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/syndtr/goleveldb/leveldb"
)

func PutHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	err = kv.db.Put(req.Key, req.Val, kv.wo)
	if err != nil {
		kv.log.Error("PutHandler", "err", err)
		rsp.Code = retcode.ErrPut
	}

	fmt.Printf("PutHandler key=%x vlen=%x code=%d\n", req.Key, md5.Sum(req.Val), rsp.Code)
	return rsp
}

func GetHandler(kv *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	rsp.Val, err = kv.db.Get(req.Key, nil)
	if err != nil {
		kv.log.Error("GetHandler", "err", err)
		if err == leveldb.ErrNotFound {
			rsp.Code = retcode.ErrNotFound
		} else {
			rsp.Code = retcode.ErrGet
		}
	}

	fmt.Printf("GetHandler key=%x vlen=%x code=%d\n", req.Key, md5.Sum(rsp.Val), rsp.Code)

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
		err error
	)

	_, err = kv.db.Get(req.Key, nil)
	if err != nil {
		kv.log.Error("HasHandler", "err", err)
		rsp.Exist = false
	}
	rsp.Exist = true

	return rsp
}
