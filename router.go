package main

import "github.com/Ankr-Shanghai/chainkv/client/pb"

type Handler func(kv *kvserver, data []byte) interface{}

var (
	handleOpts = map[string]Handler{
		pb.ReqType_REQ_TYPE_PUT.String():         PutHandler,
		pb.ReqType_REQ_TYPE_GET.String():         GetHandler,
		pb.ReqType_REQ_TYPE_DEL.String():         DelHandler,
		pb.ReqType_REQ_TYPE_HAS.String():         HasHandler,
		pb.ReqType_REQ_TYPE_BATCH_NEW.String():   NewBatchHandler,
		pb.ReqType_REQ_TYPE_BATCH_PUT.String():   BatchPutHandler,
		pb.ReqType_REQ_TYPE_BATCH_DEL.String():   BatchDelHandler,
		pb.ReqType_REQ_TYPE_BATCH_WRITE.String(): BatchWriteHandler,
		pb.ReqType_REQ_TYPE_BATCH_RESET.String(): BatchResetHandler,
		pb.ReqType_REQ_TYPE_BATCH_CLOSE.String(): BatchCloseHandler,
	}
)
