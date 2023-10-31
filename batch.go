package main

import "github.com/cockroachdb/pebble"

func NewBatch(kvs *kvserver) uint32 {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := kvs.batchIdx
	kvs.batchCache[idx] = kvs.db.NewBatch()

	return idx
}

func BatchReset(kvs *kvserver, idx uint32) {
	kvs.batchCache[idx].Reset()
}

func BatchWrite(kvs *kvserver, idx uint32) error {
	return kvs.batchCache[idx].Commit(pebble.Sync)
}

func BatchPut(kvs *kvserver, idx uint32, key, val []byte) {
	kvs.batchCache[idx].Set(key, val, pebble.NoSync)
}

func BatchDel(kvs *kvserver, idx uint32, key []byte) {
	kvs.batchCache[idx].Delete(key, pebble.NoSync)
}

func BatchClose(kvs *kvserver, idx uint32) {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()
	kvs.batchCache[idx].Close()
	delete(kvs.batchCache, idx)
}
