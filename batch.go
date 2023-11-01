package main

import (
	"crypto/md5"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

func NewBatch(kvs *kvserver) uint32 {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()

	kvs.batchIdx++
	idx := kvs.batchIdx
	kvs.batchCache[idx] = new(leveldb.Batch)

	return idx
}

func BatchReset(kvs *kvserver, idx uint32) {
	kvs.batchCache[idx].Reset()
}

func BatchWrite(kvs *kvserver, idx uint32) error {
	return kvs.db.Write(kvs.batchCache[idx], nil)
}

func BatchPut(kvs *kvserver, idx uint32, key, val []byte) {
	fmt.Printf("BatchPut: key=%x, val=%x\n", key, md5.Sum(val))
	kvs.batchCache[idx].Put(key, val)
}

func BatchDel(kvs *kvserver, idx uint32, key []byte) {
	kvs.batchCache[idx].Delete(key)
}

func BatchClose(kvs *kvserver, idx uint32) {
	kvs.batchLock.Lock()
	defer kvs.batchLock.Unlock()
	delete(kvs.batchCache, idx)
}
