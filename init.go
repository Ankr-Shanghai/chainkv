package main

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
)

func initSystem(kvs *kvserver) {
	val, closer, err := kvs.db.Get(sequenceKey)
	if err != nil {
		kvs.log.Error("sequence key is null")
		kvs.sequence = 1
	} else {
		closer.Close()
		kvs.sequence = binary.BigEndian.Uint64(val) + 1
	}
}

func cleanSystem(kvs *kvserver) {
	buf := kvs.buffer.GetLen(8)
	defer kvs.buffer.Put(buf)
	binary.BigEndian.PutUint64(buf, kvs.sequence)
	kvs.db.Set(sequenceKey, buf, pebble.Sync)
}
