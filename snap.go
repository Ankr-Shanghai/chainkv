package main

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/protobuf/proto"
)

func NewSnap(kvs *kvserver) uint32 {
	kvs.snapLock.Lock()
	defer kvs.snapLock.Unlock()

	kvs.snapIdx++
	idx := kvs.snapIdx
	snap, _ := kvs.db.GetSnapshot()
	kvs.snapCache[idx] = snap

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
		err error
		val []byte
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	val, err = kvs.snapCache[req.Id].Get(req.Key, nil)

	if err != nil {
		kvs.log.Error("SnapGetHandler", "err", err)
		rsp.Code = retcode.ErrGet
		goto END
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
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kvs.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	_, err = kvs.snapCache[req.Id].Get(req.Key, nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
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
	kvs.snapCache[req.Id].Release()
	delete(kvs.snapCache, req.Id)

END:
	out, _ := proto.Marshal(rsp)
	return out
}
