package main

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"google.golang.org/protobuf/proto"
)

func NewIteratorHandler(kvs *kvserver, data []byte) interface{} {
	req := &pb.Request{}
	rsp := &pb.Response{
		Code: retcode.CodeOK,
	}

	if err := proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("NewIteratorHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	rsp.Id = NewIter(kvs, req.Key, req.Val)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func IterNextHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("IterNextHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	rsp.Exist = IterNext(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func IterKeyHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("IterKeyHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	rsp.Val = IterKey(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func IterValHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("IterValueHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	rsp.Val = IterValue(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func IterErrorHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("IterErrorHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	rsp.Exist = IterError(kvs, req.Id) != nil

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func IterCloseHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("IterCloseHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	IterClose(kvs, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}
