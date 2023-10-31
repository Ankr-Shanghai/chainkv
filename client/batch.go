package client

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
)

type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

type Batch struct {
	client *client
	idx    uint32
	writes []keyvalue
	size   int
}

func (b *Batch) Close() error {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_CLOSE,
			Id:   b.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = b.client.do(req, rsp)
	if err != nil {
		return err
	}

	// should remove from batchMap
	b.client.batchLock.Lock()
	delete(b.client.batchMap, b.idx)
	b.client.batchLock.Unlock()

	return nil
}

// Put inserts the given value into the batch for later committing.
func (b *Batch) Put(key, value []byte) error {
	b.writes = append(b.writes, keyvalue{CopyBytes(key), CopyBytes(value), false})
	b.size += len(key) + len(value)
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_PUT,
			Key:  key,
			Val:  value,
			Id:   b.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = b.client.do(req, rsp)
	if err != nil {
		return err
	}

	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *Batch) Delete(key []byte) error {
	b.writes = append(b.writes, keyvalue{CopyBytes(key), nil, true})
	b.size += len(key)

	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_DEL,
			Key:  key,
			Id:   b.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = b.client.do(req, rsp)
	if err != nil {
		return err
	}

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *Batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to the memory database.
func (b *Batch) Write() error {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_WRITE,
			Id:   b.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = b.client.do(req, rsp)
	if err != nil {
		return err
	}

	return nil
}

// Reset resets the batch for reuse.
func (b *Batch) Reset() {
	b.writes = b.writes[:0]
	b.size = 0
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_RESET,
			Id:   b.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = b.client.do(req, rsp)
	if err != nil {
		b.client.log.Error("Batch Reset failed", "err", err)
	}
}

// CopyBytes returns an exact copy of the provided bytes.
func CopyBytes(b []byte) (copiedBytes []byte) {
	if b == nil {
		return nil
	}
	copiedBytes = make([]byte, len(b))
	copy(copiedBytes, b)

	return
}
