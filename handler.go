package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
)

type Handler func(kv *kvserver, data []byte) interface{}

var (
	handleOpts = map[string]Handler{
		pb.ReqType_REQ_TYPE_PUT.String(): PutHandler,
		pb.ReqType_REQ_TYPE_GET.String(): GetHandler,
		pb.ReqType_REQ_TYPE_DEL.String(): DelHandler,
		pb.ReqType_REQ_TYPE_HAS.String(): HasHandler,
	}
)

func PutHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.SigRequest{}
		rsp = &pb.SigResponse{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrCodeUnmarshal
		goto END
	}

	err = kv.db.Set(req.Key, req.Val, pebble.Sync)
	if err != nil {
		kv.log.Error("PutHandler", "err", err)
		rsp.Code = retcode.ErrCodePut
	}

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func GetHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.SigRequest{}
		rsp = &pb.SigResponse{
			Code: retcode.CodeOK,
		}
		dat    []byte
		closer io.Closer
		err    error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("GetHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrCodeUnmarshal
		goto END
	}

	dat, closer, err = kv.db.Get(req.Key)
	if err != nil {
		if err == pebble.ErrNotFound {
			rsp.Code = retcode.ErrCodeNotFound
		} else {
			kv.log.Error("GetHandler", "err", err)
			rsp.Code = retcode.ErrCodeGet
		}
		goto END
	}
	rsp.Val = make([]byte, len(dat))
	copy(rsp.Val, dat)
	closer.Close()

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func DelHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.SigRequest{}
		rsp = &pb.SigResponse{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("DelHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrCodeUnmarshal
		goto END
	}

	err = kv.db.Delete(req.Key, pebble.NoSync)
	if err != nil {
		kv.log.Error("DelHandler", "err", err)
		rsp.Code = retcode.ErrCodeGet
		goto END
	}
END:
	out, _ := proto.Marshal(rsp)
	return out
}

func HasHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.SigRequest{}
		rsp = &pb.SigResponse{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("HasHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrCodeUnmarshal
		goto END
	}

	_, closer, err = kv.db.Get(req.Key)
	if err != nil {
		if err == pebble.ErrNotFound {
			rsp.Code = retcode.ErrCodeNotFound
		} else {
			kv.log.Error("HasHandler", "err", err)
			rsp.Code = retcode.ErrCodeGet
		}
		goto END
	}
	closer.Close()

END:
	out, _ := proto.Marshal(rsp)
	return out
}
