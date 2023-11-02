package main

import "github.com/Ankr-Shanghai/chainkv/client/pb"

type Handler func(kv *kvserver, req *pb.Request) *pb.Response

var (
	handleOps = map[pb.ReqType]Handler{
		pb.ReqType_REQ_TYPE_PUT: PutHandler,
		pb.ReqType_REQ_TYPE_GET: GetHandler,
		pb.ReqType_REQ_TYPE_DEL: DelHandler,
		pb.ReqType_REQ_TYPE_HAS: HasHandler,
		// batch
		pb.ReqType_REQ_TYPE_BATCH_NEW:   NewBatchHandler,
		pb.ReqType_REQ_TYPE_BATCH_PUT:   BatchPutHandler,
		pb.ReqType_REQ_TYPE_BATCH_DEL:   BatchDelHandler,
		pb.ReqType_REQ_TYPE_BATCH_WRITE: BatchWriteHandler,
		pb.ReqType_REQ_TYPE_BATCH_RESET: BatchResetHandler,
		pb.ReqType_REQ_TYPE_BATCH_CLOSE: BatchCloseHandler,
		// iter
		pb.ReqType_REQ_TYPE_ITER_NEW:   NewIteratorHandler,
		pb.ReqType_REQ_TYPE_ITER_NEXT:  IterNextHandler,
		pb.ReqType_REQ_TYPE_ITER_KEY:   IterKeyHandler,
		pb.ReqType_REQ_TYPE_ITER_VAL:   IterValHandler,
		pb.ReqType_REQ_TYPE_ITER_ERROR: IterErrorHandler,
		pb.ReqType_REQ_TYPE_ITER_CLOSE: IterCloseHandler,
		// snap
		pb.ReqType_REQ_TYPE_SNAP_NEW:     NewSnapHandler,
		pb.ReqType_REQ_TYPE_SNAP_HAS:     SnapHasHandler,
		pb.ReqType_REQ_TYPE_SNAP_GET:     SnapGetHandler,
		pb.ReqType_REQ_TYPE_SNAP_RELEASE: SnapReleaseHandler,
	}
)
