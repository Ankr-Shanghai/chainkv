package main

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"google.golang.org/protobuf/proto"
)

func NewBatchHandler(kvs *kvserver, data []byte) interface{} {
	rsp := &pb.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewBatch(kvs)
	out, _ := proto.Marshal(rsp)
	return out
}

func BatchPutHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	BatchPut(kvs, req.Id, req.Key, req.Val)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func BatchDelHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}
	BatchDel(kvs, req.Id, req.Key)
END:
	out, _ := proto.Marshal(rsp)
	return out
}

func BatchWriteHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	err = BatchWrite(kvs, req.Id)
	if err != nil {
		kvs.log.Error("BatchWriteHandler", "err", err)
		rsp.Code = retcode.ErrBatchWrite
	}

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func BatchResetHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	BatchReset(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func BatchCloseHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	BatchClose(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}
