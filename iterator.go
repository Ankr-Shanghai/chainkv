package main

import (
	"github.com/cockroachdb/pebble"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iter struct {
	first bool
	iter  *pebble.Iterator
}

func NewIter(kvs *kvserver, prefix, start []byte) uint32 {
	kvs.iterLock.Lock()
	defer kvs.iterLock.Unlock()

	kvs.iterIdx++
	idx := kvs.iterIdx
	iter, _ := kvs.db.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})

	kvs.iterCache.Set(idx, &Iter{
		first: true,
		iter:  iter,
	})

	return idx
}

func IterNext(kvs *kvserver, idx uint32) bool {

	iter, _ := kvs.iterCache.Get(idx)
	if iter.first {
		iter.first = false
		return iter.iter.First()
	}
	return iter.iter.Next()
}

func IterKey(kvs *kvserver, idx uint32) []byte {
	iter, _ := kvs.iterCache.Get(idx)
	return iter.iter.Key()
}

func IterValue(kvs *kvserver, idx uint32) []byte {
	iter, _ := kvs.iterCache.Get(idx)
	return iter.iter.Value()
}

func IterClose(kvs *kvserver, idx uint32) {
	iter, _ := kvs.iterCache.Get(idx)
	iter.iter.Close()
	kvs.iterCache.Del(idx)
}

func IterError(kvs *kvserver, idx uint32) error {
	iter, _ := kvs.iterCache.Get(idx)
	return iter.iter.Error()
}

// upperBound returns the upper bound for the given prefix
func upperBound(prefix []byte) (limit []byte) {
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c == 0xff {
			continue
		}
		limit = make([]byte, i+1)
		copy(limit, prefix)
		limit[i] = c + 1
		break
	}
	return limit
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}
