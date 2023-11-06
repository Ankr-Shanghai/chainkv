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
	kvs.snapCache.Set(idx, snap)

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

	snap, _ := kvs.snapCache.Get(req.Id)
	rsp.Val, closer, err = snap.Get(req.Key)
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

	snap, _ := kvs.snapCache.Get(req.Id)
	_, closer, err = snap.Get(req.Key)
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

	snap, _ := kvs.snapCache.Get(req.Id)
	snap.Close()
	kvs.snapCache.Del(req.Id)

	return rsp
}
