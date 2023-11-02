package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func NewDatabase(datadir string) (*leveldb.DB, error) {

	var (
		cache    = 2048
		handlers = 2048
	)

	opt := &opt.Options{
		BlockCacheCapacity:     cache / 2 * opt.MiB,
		OpenFilesCacheCapacity: handlers,
		WriteBuffer:            cache / 4 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
		ReadOnly:               false,
	}

	// Open the db and recover any potential corruptions
	innerDB, err := leveldb.OpenFile(datadir, opt)
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		innerDB, err = leveldb.RecoverFile(datadir, nil)
	}
	if err != nil {
		return nil, err
	}
	return innerDB, nil
}
