package main

import (
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/cockroachdb/pebble"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iter struct {
	first bool
	iter  *pebble.Iterator
}

func NewIteratorHandler(kvs *kvserver, req *types.Request) *types.Response {
	rsp := &types.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewIter(kvs, req.Key, req.Val)
	return rsp
}

func NewIter(kvs *kvserver, prefix, start []byte) types.ID {
	kvs.iterLock.Lock()
	defer kvs.iterLock.Unlock()

	kvs.iterIdx++
	idx := types.ID(kvs.iterIdx)
	iter, _ := kvs.db.NewIter(&pebble.IterOptions{
		LowerBound: append(prefix, start...),
		UpperBound: upperBound(prefix),
	})

	kvs.iterCache.Set(idx.String(), &Iter{
		first: true,
		iter:  iter,
	})

	return idx
}

func IterNextHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterNext(kvs, req.Id.String())
	return rsp
}

func IterNext(kvs *kvserver, idx string) bool {

	iter, _ := kvs.iterCache.Get(idx)
	if iter.first {
		iter.first = false
		return iter.iter.First()
	}
	return iter.iter.Next()
}

func IterKeyHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterKey(kvs, req.Id.String())
	return rsp
}

func IterKey(kvs *kvserver, idx string) []byte {
	iter, _ := kvs.iterCache.Get(idx)
	return iter.iter.Key()
}

func IterValHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Val = IterValue(kvs, req.Id.String())
	return rsp
}

func IterValue(kvs *kvserver, idx string) []byte {
	iter, _ := kvs.iterCache.Get(idx)
	return iter.iter.Value()
}

func IterCloseHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	IterClose(kvs, req.Id.String())
	return rsp
}

func IterClose(kvs *kvserver, idx string) {
	iter, _ := kvs.iterCache.Get(idx)
	kvs.closer <- func() {
		iter.iter.Close()
		kvs.iterCache.Remove(idx)
	}
}

func IterErrorHandler(kvs *kvserver, req *types.Request) *types.Response {
	var (
		rsp = &types.Response{
			Code: retcode.CodeOK,
		}
	)
	rsp.Exist = IterError(kvs, req.Id.String()) != nil
	return rsp
}

func IterError(kvs *kvserver, idx string) error {
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
