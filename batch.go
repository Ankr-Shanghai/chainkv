package main

import (
	"github.com/Ankr-Shanghai/chainkv/types"
)

func NewBatch(kvs *kvserver) types.ID {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := types.ID(kvs.batchIdx)

	kvs.batchCache.Set(idx.String(), kvs.db.NewBatch())

	return idx
}

func BatchReset(kvs *kvserver, idx string) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Close()
	nb := kvs.db.NewBatch()
	kvs.batchCache.Set(idx, nb)
}

func BatchWrite(kvs *kvserver, idx string) error {
	batch, _ := kvs.batchCache.Get(idx)
	return batch.Commit(kvs.wo)
}

func BatchPut(kvs *kvserver, idx string, key, val []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Set(key, val, kvs.wo)
}

func BatchDel(kvs *kvserver, idx string, key []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Delete(key, kvs.wo)
}

func BatchClose(kvs *kvserver, idx string) {
	batch, _ := kvs.batchCache.Get(idx)
	kvs.closer <- func() {
		batch.Close()
		kvs.batchCache.Remove(idx)
	}
}
