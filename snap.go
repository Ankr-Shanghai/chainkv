package main

import (
	"io"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
)

func NewSnap(kvs *kvserver) uint32 {
	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()

	kvs.snapIdx++
	idx := kvs.snapIdx
	kvs.snapCache[idx] = kvs.db.NewSnapshot()

	return idx
}

func NewSnapHandler(kvs *kvserver, data []byte) interface{} {
	rsp := &pb.Response{
		Code: retcode.CodeOK,
	}
	rsp.Id = NewSnap(kvs)
	out, _ := proto.Marshal(rsp)
	return out
}

func SnapGetHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err    error
		closer io.Closer
		val    []byte
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	val, closer, _ = kvs.snapCache[req.Id].Get(req.Key)
	if closer != nil {
		defer closer.Close()
	}
	rsp.Val = val

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func SnapHasHandler(kvs *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err    error
		closer io.Closer
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	_, closer, err = kvs.snapCache[req.Id].Get(req.Key)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		if err != pebble.ErrNotFound {
			rsp.Exist = false
			rsp.Code = retcode.ErrGet
		} else {
			rsp.Exist = false
		}
	}

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func SnapReleaseHandler(kvs *kvserver, data []byte) interface{} {
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

	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()
	kvs.snapCache[req.Id].Close()
	delete(kvs.snapCache, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}
