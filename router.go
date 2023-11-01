package main

import "github.com/Ankr-Shanghai/chainkv/client/pb"

type Handler func(kv *kvserver, data []byte) interface{}

var (
	handleOps = map[string]Handler{
		pb.ReqType_REQ_TYPE_PUT.String():          PutHandler,
		pb.ReqType_REQ_TYPE_GET.String():          GetHandler,
		pb.ReqType_REQ_TYPE_DEL.String():          DelHandler,
		pb.ReqType_REQ_TYPE_HAS.String():          HasHandler,
		pb.ReqType_REQ_TYPE_BATCH_NEW.String():    NewBatchHandler,
		pb.ReqType_REQ_TYPE_BATCH_PUT.String():    BatchPutHandler,
		pb.ReqType_REQ_TYPE_BATCH_DEL.String():    BatchDelHandler,
		pb.ReqType_REQ_TYPE_BATCH_WRITE.String():  BatchWriteHandler,
		pb.ReqType_REQ_TYPE_BATCH_RESET.String():  BatchResetHandler,
		pb.ReqType_REQ_TYPE_BATCH_CLOSE.String():  BatchCloseHandler,
		pb.ReqType_REQ_TYPE_ITER_NEW.String():     NewIteratorHandler,
		pb.ReqType_REQ_TYPE_ITER_NEXT.String():    IterNextHandler,
		pb.ReqType_REQ_TYPE_ITER_KEY.String():     IterKeyHandler,
		pb.ReqType_REQ_TYPE_ITER_VAL.String():     IterValHandler,
		pb.ReqType_REQ_TYPE_ITER_ERROR.String():   IterErrorHandler,
		pb.ReqType_REQ_TYPE_ITER_CLOSE.String():   IterCloseHandler,
		pb.ReqType_REQ_TYPE_SNAP_NEW.String():     NewSnapHandler,
		pb.ReqType_REQ_TYPE_SNAP_HAS.String():     SnapHasHandler,
		pb.ReqType_REQ_TYPE_SNAP_GET.String():     SnapGetHandler,
		pb.ReqType_REQ_TYPE_SNAP_RELEASE.String(): SnapReleaseHandler,
	}
)
