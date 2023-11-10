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

func NewBatch(kvs *kvserver) types.ID {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := types.ID(kvs.batchIdx)

	kvs.batchCache.Set(idx.String(), kvs.db.NewBatch())

	return idx
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

func BatchReset(kvs *kvserver, idx string) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Close()
	nb := kvs.db.NewBatch()
	kvs.batchCache.Set(idx, nb)
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

func BatchWrite(kvs *kvserver, idx string) error {
	batch, _ := kvs.batchCache.Get(idx)
	return batch.Commit(kvs.wo)
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

func BatchPut(kvs *kvserver, idx string, key, val []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Set(key, val, kvs.wo)
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
func BatchDel(kvs *kvserver, idx string, key []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Delete(key, kvs.wo)
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
func BatchClose(kvs *kvserver, idx string) {
	batch, _ := kvs.batchCache.Get(idx)
	kvs.closer <- func() {
		batch.Close()
		kvs.batchCache.Remove(idx)
	}
}
