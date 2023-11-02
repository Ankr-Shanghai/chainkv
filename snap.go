package main

import (
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewSnap(kvs *kvserver) uint32 {
	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()

	kvs.snapIdx++
	idx := kvs.snapIdx
	snap, _ := kvs.db.GetSnapshot()
	kvs.snapCache[idx] = snap

	return idx
}

func NewSnapHandler(kvs *kvserver, req *types.Request) *types.Response {
	rsp := &types.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewSnap(kvs)
	return rsp
}

func SnapGetHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	rsp.Val, err = kvs.snapCache[req.Id].Get(req.Key, nil)
	if err != nil {
		kvs.log.Error("SnapGetHandler", "err", err)
		rsp.Code = retcode.ErrGet
	}

	return rsp
}

func SnapHasHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	_, err = kvs.snapCache[req.Id].Get(req.Key, nil)
	if err != nil {
		rsp.Exist = false
		rsp.Code = retcode.ErrGet
	}

	return rsp
}

func SnapReleaseHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)

	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()
	kvs.snapCache[req.Id].Release()
	delete(kvs.snapCache, req.Id)

	return rsp
}
