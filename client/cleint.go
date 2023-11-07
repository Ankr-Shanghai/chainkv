package client

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/Ankr-Shanghai/chainkv/client/pool"
	"github.com/Ankr-Shanghai/chainkv/codec"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/gobwas/pool/pbytes"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type client struct {
	src    string // remote address:
	pool   pool.Pool
	log    *slog.Logger
	buffer *pbytes.Pool
	codec  *codec.Codec

	// batchMap is used to store the batch object
	batchMap cmap.ConcurrentMap[string, *Batch]

	// itermap is used to store the iterator object
	iterMap cmap.ConcurrentMap[string, *Iterator]

	// snapMap is used to store the snap object
	snapMap cmap.ConcurrentMap[string, *Snap]
}

type Option struct {
	Host string
	Port string
	Size int // client pool size
}

func NewClient(opt *Option) (*client, error) {
	src := fmt.Sprintf("%s:%s", opt.Host, opt.Port)
	factory := func() (net.Conn, error) {
		return net.Dial("tcp", src)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	p, err := pool.NewPool(opt.Size, opt.Size, factory, nil)
	if err != nil {
		logger.Error("NewClient", "err", err)
		return nil, err
	}

	buffer := pbytes.New(128, 1024*1024*64)

	return &client{
		src:      src,
		pool:     p,
		log:      logger,
		buffer:   buffer,
		batchMap: cmap.New[*Batch](),
		iterMap:  cmap.New[*Iterator](),
		snapMap:  cmap.New[*Snap](),
		codec:    &codec.Codec{},
	}, nil
}

func (c *client) flush() error {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_FLUSH,
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
			Type: types.REQ_TYPE_SNAP_NEW,
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

	c.snapMap.Set(rsp.Id.String(), snap)

	return snap, nil
}

func (c *client) NewIter(prefix, start []byte) (*Iterator, error) {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_NEW,
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

	c.iterMap.Set(rsp.Id.String(), iter)

	return iter, nil
}

func (c *client) NewBatch() (*Batch, error) {

	var (
		req = &types.Request{
			Type: types.REQ_TYPE_BATCH_NEW,
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

	c.batchMap.Set(rsp.Id.String(), batch)

	return batch, nil
}

func (c *client) Close() error {

	// close all batch
	c.log.Info("client close", "batch count: ", c.batchMap.Count())
	c.batchMap.IterCb(func(key string, value *Batch) {
		println("1")
		value.Close()
		println("2")
	})

	// close all iterator
	c.log.Info("client close", "iter count: ", c.iterMap.Count())
	c.iterMap.IterCb(func(key string, value *Iterator) {
		value.Close()
	})

	// close all snap
	c.log.Info("snap close", "iter count: ", c.snapMap.Count())
	c.snapMap.IterCb(func(key string, value *Snap) {
		value.Release()
	})

	err := c.flush()
	if err != nil {
		return err
	}
	c.log.Info("client close flush")

	// must be close last
	c.pool.Close()

	return nil
}

func (c *client) Get(key []byte) ([]byte, error) {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_GET,
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
			Type: types.REQ_TYPE_PUT,
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
			Type: types.REQ_TYPE_DEL,
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
			Type: types.REQ_TYPE_HAS,
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

	reqs := req.Marshal()
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
	buf = buf[:0]
	cache := c.buffer.GetLen(4 * 1024)
	defer func() {
		c.buffer.Put(buf)
		c.buffer.Put(cache)
	}()

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

	err = rsp.Unmarshal(rs)
	if err != nil {
		c.log.Error("Unmarshal failed", "err", err, "len", total)
		return err
	}

	return nil
}
