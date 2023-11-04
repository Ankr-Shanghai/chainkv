package main

import (
	"github.com/Ankr-Shanghai/chainkv/types"
)

type Handler func(kv *kvserver, req *types.Request) *types.Response

var (
	handleOps = map[types.ReqType]Handler{
		types.REQ_TYPE_PUT:   PutHandler,
		types.REQ_TYPE_GET:   GetHandler,
		types.REQ_TYPE_DEL:   DelHandler,
		types.REQ_TYPE_HAS:   HasHandler,
		types.REQ_TYPE_FLUSH: FlushDBHandler,
		// batch
		types.REQ_TYPE_BATCH_NEW:   NewBatchHandler,
		types.REQ_TYPE_BATCH_PUT:   BatchPutHandler,
		types.REQ_TYPE_BATCH_DEL:   BatchDelHandler,
		types.REQ_TYPE_BATCH_WRITE: BatchWriteHandler,
		types.REQ_TYPE_BATCH_RESET: BatchResetHandler,
		types.REQ_TYPE_BATCH_CLOSE: BatchCloseHandler,
		// iter
		types.REQ_TYPE_ITER_NEW:   NewIteratorHandler,
		types.REQ_TYPE_ITER_NEXT:  IterNextHandler,
		types.REQ_TYPE_ITER_KEY:   IterKeyHandler,
		types.REQ_TYPE_ITER_VAL:   IterValHandler,
		types.REQ_TYPE_ITER_ERROR: IterErrorHandler,
		types.REQ_TYPE_ITER_CLOSE: IterCloseHandler,
		// snap
		types.REQ_TYPE_SNAP_NEW:     NewSnapHandler,
		types.REQ_TYPE_SNAP_HAS:     SnapHasHandler,
		types.REQ_TYPE_SNAP_GET:     SnapGetHandler,
		types.REQ_TYPE_SNAP_RELEASE: SnapReleaseHandler,
	}
)
