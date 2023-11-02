package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewSnap(kvs *kvserver) uint32 {
	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()

	kvs.snapIdx++
	idx := kvs.snapIdx
	snap := kvs.db.NewSnapshot()
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
	kvs.snapLock.RLock()
	defer kvs.snapLock.RUnlock()

	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	rsp.Val, closer, err = kvs.snapCache[req.Id].Get(req.Key)
	if err != nil {
		kvs.log.Error("SnapGetHandler", "err", err)
		rsp.Code = retcode.ErrGet
	}

	if closer != nil {
		closer.Close()
	}

	return rsp
}

func SnapHasHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	_, closer, err = kvs.snapCache[req.Id].Get(req.Key)
	if err != nil {
		rsp.Exist = false
		rsp.Code = retcode.ErrGet
	}

	if closer != nil {
		closer.Close()
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
	kvs.snapCache[req.Id].Close()
	delete(kvs.snapCache, req.Id)

	return rsp
}
