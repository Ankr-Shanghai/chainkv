package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Ankr-Shanghai/chainkv/codec"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/cockroachdb/pebble"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/panjf2000/gnet/v2"
)

type kvserver struct {
	gnet.BuiltinEventEngine
	eng  gnet.Engine
	log  *slog.Logger
	addr string

	db *pebble.DB
	wo *pebble.WriteOptions

	batchLock  sync.Mutex
	batchIdx   uint32
	batchCache cmap.ConcurrentMap[string, *pebble.Batch]

	iterLock  sync.Mutex
	iterIdx   uint32
	iterCache cmap.ConcurrentMap[string, *Iter]

	snapLock  sync.Mutex
	snapIdx   uint32
	snapCache cmap.ConcurrentMap[string, *pebble.Snapshot]
}

func NewServer(host, port, datadir string) (*kvserver, error) {

	var err error
	s := &kvserver{
		log: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		batchCache: cmap.New[*pebble.Batch](),
		iterCache:  cmap.New[*Iter](),
		snapCache:  cmap.New[*pebble.Snapshot](),
		addr:       fmt.Sprintf("tcp://%s:%s", host, port),
	}

	s.wo = pebble.Sync

	// open the database
	db, err := NewPebble(datadir)
	if err != nil {
		return nil, err
	}
	s.db = db

	return s, nil
}

func (s *kvserver) Stop(ctx context.Context) {
	s.eng.Stop(ctx)
}

func (s *kvserver) OnShutdown(c gnet.Engine) {
	s.log.Info("server shutdown and clean all resources...")

	s.iterCache.IterCb(func(key string, value *Iter) {
		value.iter.Close()
	})

	s.batchCache.IterCb(func(key string, value *pebble.Batch) {
		value.Close()
	})

	s.snapCache.IterCb(func(key string, value *pebble.Snapshot) {
		value.Close()
	})

	err := s.db.Close()
	if err != nil {
		s.log.Error("kvserver stop", "err", err)
	}

	return
}

func (s *kvserver) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.log.Info("server booting ...", "addr", s.addr)
	s.eng = eng
	return
}

func (s *kvserver) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c.SetContext(&codec.Codec{})
	s.log.Info("OnConnect", "Total", s.eng.CountConnections(), "remoteAddr", c.RemoteAddr())
	return
}

func (s *kvserver) OnTraffic(c gnet.Conn) (action gnet.Action) {
	code := c.Context().(*codec.Codec)
	// read all data from the buffer
	data, err := code.Decode(c)
	if err == codec.ErrIncompletePacket {
		return
	}
	if err != nil {
		s.log.Error("OnTraffic recieve", "err", err)
		return gnet.Close
	}
	req := &types.Request{}
	err = req.Unmarshal(data)
	if err != nil {
		s.log.Error("OnTraffic unmarshal", "err", err)
		return
	}

	handler, ok := handleOps[req.Type]
	if !ok {
		rsp := &types.Response{Code: retcode.ErrNotSupport}
		data = rsp.Marshal()
		c.Write(data)
		return
	}
	rsp := handler(s, req)
	rs := rsp.Marshal()
	lst, err := code.Encode(rs)
	if err != nil {
		s.log.Error("OnTraffic encode", "err", err)
		return
	}
	c.Write(lst)
	return
}

func (s *kvserver) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	s.log.Info("OnClose", "total", s.eng.CountConnections(), "remoteAddr", c.RemoteAddr())
	return gnet.Close
}
