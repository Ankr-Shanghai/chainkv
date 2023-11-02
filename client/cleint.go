package client

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/Ankr-Shanghai/chainkv/client/pool"
	"github.com/Ankr-Shanghai/chainkv/codec"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/gobwas/pool/pbytes"
	"github.com/vmihailenco/msgpack/v5"
)

type client struct {
	src    string // remote address:
	pool   pool.Pool
	log    *slog.Logger
	buffer *pbytes.Pool
	codec  *codec.Codec

	// batchMap is used to store the batch object
	batchLock sync.Mutex
	batchMap  map[uint32]*Batch

	// itermap is used to store the iterator object
	iterLock sync.Mutex
	iterMap  map[uint32]*Iterator

	// snapMap is used to store the snap object
	snapLock sync.Mutex
	snapMap  map[uint32]*Snap
}

type Option struct {
	Host string
	Port string
	Size int // client pool size
}

func NewClient(opt *Option) (*client, error) {
	src := fmt.Sprintf("%s:%s", opt.Host, opt.Port)
	functory := func() (net.Conn, error) {
		return net.Dial("tcp", src)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	p, err := pool.NewPool(opt.Size, opt.Size, functory, nil)
	if err != nil {
		logger.Error("NewClient", "err", err)
		return nil, err
	}

	buffer := pbytes.New(128, 1024*1024*128)

	return &client{
		src:      src,
		pool:     p,
		log:      logger,
		buffer:   buffer,
		batchMap: make(map[uint32]*Batch),
		iterMap:  make(map[uint32]*Iterator),
		snapMap:  make(map[uint32]*Snap),
		codec:    &codec.Codec{},
	}, nil
}

func (c *client) flush() error {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_FLUSH,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) NewSnap() (*Snap, error) {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_SNAP_NEW,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return nil, ErrNewSnap
	}

	snap := &Snap{
		client: c,
		idx:    rsp.Id,
	}

	c.snapLock.Lock()
	c.snapMap[rsp.Id] = snap
	c.snapLock.Unlock()

	return snap, nil
}

func (c *client) NewIter(prefix, start []byte) (*Iterator, error) {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_ITER_NEW,
			Key:  prefix,
			Val:  start,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{
		client: c,
		idx:    rsp.Id,
	}

	c.iterLock.Lock()
	c.iterMap[rsp.Id] = iter
	c.iterLock.Unlock()

	return iter, nil
}

func (c *client) NewBatch() (*Batch, error) {

	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_BATCH_NEW,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)
	err = c.do(req, rsp)
	if err != nil {
		return nil, ErrNewBatch
	}
	batch := &Batch{
		client: c,
		idx:    rsp.Id,
		Writes: make([]KeyValue, 0),
	}

	c.batchLock.Lock()
	c.batchMap[rsp.Id] = batch
	c.batchLock.Unlock()

	return batch, nil
}

func (c *client) Close() error {

	// close all batch
	for _, batch := range c.batchMap {
		batch.Close()
	}
	// close all iterator
	for _, iter := range c.iterMap {
		iter.Close()
	}
	// close all snap
	for _, snap := range c.snapMap {
		snap.Release()
	}

	err := c.flush()
	if err != nil {
		return err
	}

	// must be close last
	c.pool.Close()

	return nil
}

func (c *client) Get(key []byte) ([]byte, error) {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_GET,
			Key:  key,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return nil, errors.New("get failed")
	}

	return rsp.Val, nil
}

func (c *client) Put(key, value []byte) error {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_PUT,
			Key:  key,
			Val:  value,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return errors.New("put failed")
	}

	return nil
}

func (c *client) Delete(key []byte) error {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_DEL,
			Key:  key,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return errors.New("del failed")
	}
	return nil
}

func (c *client) Has(key []byte) (bool, error) {
	var (
		req = &types.Request{
			Type: types.ReqType_REQ_TYPE_GET,
			Key:  key,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return false, errors.New("get failed")
	}

	if rsp.Code == retcode.ErrNotFound {
		return false, nil
	}

	return rsp.Exist, nil
}

func (c *client) do(req *types.Request, rsp *types.Response) error {
	conn, err := c.pool.Get()
	if err != nil {
		c.log.Error("Get connection failed", "err", err)
		return err
	}
	defer conn.Close()

	reqs, _ := msgpack.Marshal(req)
	ret, err := c.codec.Encode(reqs)
	if err != nil {
		c.log.Error("Encode failed", "err", err)
		return err
	}

	_, err = conn.Write(ret)
	if err != nil {
		c.log.Error("Write failed", "err", err)
		return err
	}

	buf := c.buffer.GetLen(1024 * 1024 * 4)
	defer c.buffer.Put(buf)
	buf = buf[:0]

	cache := c.buffer.GetLen(4 * 1024)
	defer c.buffer.Put(cache)
	var (
		total = 0
		rs    []byte
	)

	// read from connection
	for {
		wn, err := conn.Read(cache)
		if err != nil {
			return err
		}
		total += wn
		buf = append(buf, cache[:wn]...)
		rs, err = c.codec.Unpack(buf[:total])
		if err != nil {
			continue
		} else {
			break
		}
	}

	err = msgpack.Unmarshal(rs, rsp)
	if err != nil {
		c.log.Error("Unmarshal failed", "err", err, "len", total)
		return err
	}

	return nil
}
