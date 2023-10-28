package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
)

type Handler func(kv *kvserver, data []byte) interface{}

var (
	handleOpts = map[string]Handler{
		"PutRequest": PutHandler,
		"GetRequest": GetHandler,
	}
)

func PutHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.PutRequest{}
		rsp = &pb.PutResponse{
			Code: CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = ErrCodeUnmarshal
		goto END
	}
	kv.log.Info("PutHandler", "key", string(req.Key), "value", string(req.Value))

	err = kv.db.Set(req.Key, req.Value, pebble.Sync)
	if err != nil {
		kv.log.Error("PutHandler", "err", err)
		rsp.Code = ErrCodePut
	}

END:
	kv.log.Info("PutHandler", "rsp", rsp)
	out, _ := proto.Marshal(rsp)
	kv.log.Info("PutHandler", "out", out)
	return out
}

func GetHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.GetRequest{}
		rsp = &pb.GetResponse{
			Code: CodeOK,
		}
		dat    []byte
		closer io.Closer
		err    error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("GetHandler unmarshal request", "err", err)
		rsp.Code = ErrCodeUnmarshal
		goto END
	}

	dat, closer, err = kv.db.Get(req.Key)
	if err != nil {
		kv.log.Error("GetHandler", "err", err)
		rsp.Code = ErrCodeGet
		goto END
	}
	rsp.Value = make([]byte, len(dat))
	copy(rsp.Value, dat)
	closer.Close()

END:
	out, _ := proto.Marshal(rsp)
	return out
}
