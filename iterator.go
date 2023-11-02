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

	kvs.iterCache[idx] = &Iter{
		first: true,
		iter:  iter,
	}

	return idx
}

func IterNext(kvs *kvserver, idx uint32) bool {
	kvs.iterLock.RLock()
	defer kvs.iterLock.RUnlock()

	if kvs.iterCache[idx].first {
		kvs.iterCache[idx].first = false
		return kvs.iterCache[idx].iter.First()
	}
	return kvs.iterCache[idx].iter.Next()
}

func IterKey(kvs *kvserver, idx uint32) []byte {
	kvs.iterLock.RLock()
	defer kvs.iterLock.RUnlock()

	iter := kvs.iterCache[idx]
	return iter.iter.Key()
}

func IterValue(kvs *kvserver, idx uint32) []byte {
	kvs.iterLock.RLock()
	defer kvs.iterLock.RUnlock()

	iter := kvs.iterCache[idx]
	return iter.iter.Value()
}

func IterClose(kvs *kvserver, idx uint32) {
	kvs.iterLock.Lock()
	defer kvs.iterLock.Unlock()

	iter := kvs.iterCache[idx]
	iter.iter.Close()
	delete(kvs.iterCache, idx)
}

func IterError(kvs *kvserver, idx uint32) error {
	kvs.iterLock.RLock()
	defer kvs.iterLock.RUnlock()

	iter := kvs.iterCache[idx]
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
