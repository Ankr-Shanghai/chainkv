package main

func NewBatch(kvs *kvserver) uint32 {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := kvs.batchIdx
	kvs.batchCache[idx] = kvs.db.NewBatch()

	return idx
}

func BatchReset(kvs *kvserver, idx uint32) {
	kvs.batchLock.RLock()
	defer kvs.batchLock.RUnlock()
	kvs.batchCache[idx].Reset()
}

func BatchWrite(kvs *kvserver, idx uint32) error {
	kvs.batchLock.RLock()
	defer kvs.batchLock.RUnlock()
	return kvs.batchCache[idx].Commit(kvs.wo)
}

func BatchPut(kvs *kvserver, idx uint32, key, val []byte) {
	kvs.batchLock.RLock()
	defer kvs.batchLock.RUnlock()
	kvs.batchCache[idx].Set(key, val, kvs.wo)
}

func BatchDel(kvs *kvserver, idx uint32, key []byte) {
	kvs.batchLock.RLock()
	defer kvs.batchLock.RUnlock()
	kvs.batchCache[idx].Delete(key, kvs.wo)
}

func BatchClose(kvs *kvserver, idx uint32) {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()
	delete(kvs.batchCache, idx)
}
