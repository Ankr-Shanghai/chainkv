package main

import (
	"github.com/Ankr-Shanghai/chainkv/types"
)

type Handler func(kv *kvserver, req *types.Request) *types.Response

var (
	handleOps = map[types.ReqType]Handler{
		types.ReqType_REQ_TYPE_PUT: PutHandler,
		types.ReqType_REQ_TYPE_GET: GetHandler,
		types.ReqType_REQ_TYPE_DEL: DelHandler,
		types.ReqType_REQ_TYPE_HAS: HasHandler,
		// batch
		types.ReqType_REQ_TYPE_BATCH_NEW:   NewBatchHandler,
		types.ReqType_REQ_TYPE_BATCH_PUT:   BatchPutHandler,
		types.ReqType_REQ_TYPE_BATCH_DEL:   BatchDelHandler,
		types.ReqType_REQ_TYPE_BATCH_WRITE: BatchWriteHandler,
		types.ReqType_REQ_TYPE_BATCH_RESET: BatchResetHandler,
		types.ReqType_REQ_TYPE_BATCH_CLOSE: BatchCloseHandler,
		// iter
		types.ReqType_REQ_TYPE_ITER_NEW:   NewIteratorHandler,
		types.ReqType_REQ_TYPE_ITER_NEXT:  IterNextHandler,
		types.ReqType_REQ_TYPE_ITER_KEY:   IterKeyHandler,
		types.ReqType_REQ_TYPE_ITER_VAL:   IterValHandler,
		types.ReqType_REQ_TYPE_ITER_ERROR: IterErrorHandler,
		types.ReqType_REQ_TYPE_ITER_CLOSE: IterCloseHandler,
		// snap
		types.ReqType_REQ_TYPE_SNAP_NEW:     NewSnapHandler,
		types.ReqType_REQ_TYPE_SNAP_HAS:     SnapHasHandler,
		types.ReqType_REQ_TYPE_SNAP_GET:     SnapGetHandler,
		types.ReqType_REQ_TYPE_SNAP_RELEASE: SnapReleaseHandler,
	}
)
