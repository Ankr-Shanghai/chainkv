package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/client/pool"
	"github.com/Ankr-Shanghai/chainkv/plugins"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/gobwas/pool/pbytes"
	"google.golang.org/protobuf/proto"
)

type client struct {
	src    string // remote address:
	pool   pool.Pool
	log    *slog.Logger
	buffer *pbytes.Pool

	// batchMap is used to store the batch object
	batchLock sync.Mutex
	batchMap  map[uint32]*Batch

	// itermap is used to store the iterator object
	iterLock sync.Mutex
	iterMap  map[uint32]*Iterator
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
	}, nil
}

func (c *client) NewIter(prefix, start []byte) (*Iterator, error) {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_ITER_NEW,
			Key:  prefix,
			Val:  start,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
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

	return iter, nil
}

func (c *client) NewBatch() (*Batch, error) {

	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_BATCH_NEW,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)
	err = c.do(req, rsp)
	if err != nil {
		return nil, ErrNewBatch
	}
	batch := &Batch{
		client: c,
		idx:    rsp.Id,
		writes: make([]keyvalue, 0),
	}

	c.batchLock.Lock()
	c.batchMap[rsp.Id] = batch
	c.batchLock.Unlock()
	c.log.Info("NewBatch", "idx", rsp.Id)
	return batch, nil
}

func (c *client) Close() error {

	// close all batch
	c.batchLock.Lock()
	for _, batch := range c.batchMap {
		batch.Close()
	}
	c.batchLock.Unlock()

	// close all iterator
	c.iterLock.Lock()
	for _, iter := range c.iterMap {
		iter.Close()
	}
	c.iterLock.Unlock()

	// must be close last
	c.pool.Close()

	return nil
}

func (c *client) Get(key []byte) ([]byte, error) {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_GET,
			Key:  key,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return nil, errors.New("get failed")
	}

	if rsp.Code == retcode.ErrNotFound {
		return nil, ErrNotFound
	}
	return rsp.Val, nil
}

func (c *client) Put(key, value []byte) error {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_PUT,
			Key:  key,
			Val:  value,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
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
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_DEL,
			Key:  key,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
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
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_GET,
			Key:  key,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
		err error
	)

	err = c.do(req, rsp)
	if err != nil {
		return false, errors.New("get failed")
	}

	if rsp.Code == retcode.ErrNotFound {
		return false, ErrNotFound
	}
	return true, nil
}

func (c *client) do(req *pb.Request, rsp *pb.Response) error {
	conn, err := c.pool.Get()
	if err != nil {
		c.log.Error("Get connection failed", "err", err)
		return err
	}
	defer conn.Close()

	reqs, _ := proto.Marshal(req)

	ret := plugins.PackMessage(req.Type.String(), reqs)
	_, err = conn.Write(ret)
	if err != nil {
		c.log.Error("Write failed", "err", err)
		return err
	}

	mlen := c.buffer.GetLen(4)
	defer c.buffer.Put(mlen)

	_, err = conn.Read(mlen)
	if err != nil {
		c.log.Error("Read failed", "err", err)
		return err
	}

	msgLen := binary.BigEndian.Uint32(mlen)
	buf := c.buffer.GetLen(int(msgLen))
	defer c.buffer.Put(buf)

	_, err = conn.Read(buf)
	if err != nil {
		return err
	}

	err = proto.Unmarshal(buf, rsp)
	if err != nil {
		return err
	}

	return nil
}
