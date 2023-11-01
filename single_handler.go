package main

import (
	"crypto/md5"
	"fmt"
	"io"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/protobuf/proto"
)

func PutHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("PutHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	fmt.Printf("Single Put: key=%x, val=%x\n", req.Key, md5.Sum(req.Val))
	err = kv.db.Put(req.Key, req.Val, nil)
	if err != nil {
		kv.log.Error("PutHandler", "err", err)
		rsp.Code = retcode.ErrPut
	}

END:
	out, _ := proto.Marshal(rsp)
	return out
}

func GetHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		dat []byte
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("GetHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	dat, err = kv.db.Get(req.Key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			rsp.Code = retcode.ErrNotFound
		} else {
			kv.log.Error("GetHandler", "err", err)
			rsp.Code = retcode.ErrGet
		}
		goto END
	}
	rsp.Val = make([]byte, len(dat))
	copy(rsp.Val, dat)

	fmt.Printf("Single Get: key=%x, val=%x\n", req.Key, md5.Sum(req.Val))
END:
	out, _ := proto.Marshal(rsp)
	return out
}

func DelHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		err error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("DelHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	err = kv.db.Delete(req.Key, nil)
	if err != nil {
		kv.log.Error("DelHandler", "err", err)
		rsp.Code = retcode.ErrGet
		goto END
	}
END:
	out, _ := proto.Marshal(rsp)
	return out
}

func HasHandler(kv *kvserver, data []byte) interface{} {
	var (
		req = &pb.Request{}
		rsp = &pb.Response{
			Code: retcode.CodeOK,
		}
		closer io.Closer
		err    error
	)

	if err = proto.Unmarshal(data, req); err != nil {
		kv.log.Error("HasHandler unmarshal request", "err", err)
		rsp.Code = retcode.ErrUnmarshal
		goto END
	}

	_, err = kv.db.Get(req.Key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			rsp.Code = retcode.ErrNotFound
		} else {
			kv.log.Error("HasHandler", "err", err)
			rsp.Code = retcode.ErrGet
		}
		goto END
	}
	closer.Close()

END:
	out, _ := proto.Marshal(rsp)
	return out
}
