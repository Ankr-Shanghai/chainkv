package main

import (
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
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

func NewPebble(datadir string) (*pebble.DB, error) {

	var (
		cache    uint64 = 2048
		handlers        = 128
		// The max memtable size is limited by the uint32 offsets stored in
		// internal/arenaskl.node, DeferredBatchOp, and flushableBatchEntry.
		// Taken from https://github.com/cockroachdb/pebble/blob/master/open.go#L38
		maxMemTableSize uint64 = 2<<30 - 1 // Capped by 4 GB
		memTableLimit          = 2
	)

	opt := &pebble.Options{
		// Pebble has a single combined cache area and the write
		// buffers are taken from this too. Assign all available
		// memory allowance for cache.
		Cache:        pebble.NewCache(int64(cache * 1024 * 1024)),
		MaxOpenFiles: handlers,

		// The size of memory table(as well as the write buffer).
		// Note, there may have more than two memory tables in the system.
		MemTableSize: maxMemTableSize / 2,

		// MemTableStopWritesThreshold places a hard limit on the size
		// of the existent MemTables(including the frozen one).
		// Note, this must be the number of tables not the size of all memtables
		// according to https://github.com/cockroachdb/pebble/blob/master/options.go#L738-L742
		// and to https://github.com/cockroachdb/pebble/blob/master/db.go#L1892-L1903.
		MemTableStopWritesThreshold: memTableLimit,

		// The default compaction concurrency(1 thread),
		// Here use all available CPUs for faster compaction.
		MaxConcurrentCompactions: func() int { return runtime.NumCPU() },

		// Per-level options. Options for at least one level must be specified. The
		// options for the last level are used for all subsequent levels.
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10)},
		},
		ReadOnly: false,
	}
	// Disable seek compaction explicitly. Check https://github.com/ethereum/go-ethereum/pull/20130
	// for more details.
	opt.Experimental.ReadSamplingMultiplier = -1

	// Open the db and recover any potential corruptions
	innerDB, err := pebble.Open(datadir, opt)
	if err != nil {
		return nil, err
	}
	return innerDB, nil
}
