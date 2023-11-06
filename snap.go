package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewSnap(kvs *kvserver) types.ID {
	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()

	kvs.snapIdx++
	idx := types.ID(kvs.snapIdx)
	snap := kvs.db.NewSnapshot()
	kvs.snapCache.Set(idx.String(), snap)

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
		closer io.Closer
		err    error
	)

	snap, _ := kvs.snapCache.Get(req.Id.String())
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

	snap, _ := kvs.snapCache.Get(req.Id.String())
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

	snap, _ := kvs.snapCache.Get(req.Id.String())
	kvs.snapCache.Remove(req.Id.String())
	snap.Close()

	return rsp
}
