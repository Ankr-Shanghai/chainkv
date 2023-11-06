package main

func NewBatch(kvs *kvserver) uint32 {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := kvs.batchIdx

	kvs.batchCache.Set(idx, kvs.db.NewBatch())

	return idx
}

func BatchReset(kvs *kvserver, idx uint32) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Reset()
}

func BatchWrite(kvs *kvserver, idx uint32) error {
	batch, _ := kvs.batchCache.Get(idx)
	return batch.Commit(kvs.wo)
}

func BatchPut(kvs *kvserver, idx uint32, key, val []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Set(key, val, kvs.wo)
}

func BatchDel(kvs *kvserver, idx uint32, key []byte) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Delete(key, kvs.wo)
}

func BatchClose(kvs *kvserver, idx uint32) {
	batch, _ := kvs.batchCache.Get(idx)
	batch.Close()
	kvs.batchCache.Del(idx)
}
