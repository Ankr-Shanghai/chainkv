package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Ankr-Shanghai/chainkv/codec"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
	"github.com/cockroachdb/pebble"
	"github.com/gobwas/pool/pbytes"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/panjf2000/gnet/v2"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type kvserver struct {
	gnet.BuiltinEventEngine
	eng  gnet.Engine
	log  *slog.Logger
	addr string

	db       *pebble.DB
	wo       *pebble.WriteOptions
	seqLock  sync.Mutex
	sequence uint64
	buffer   *pbytes.Pool

	batchLock  sync.Mutex
	batchIdx   uint32
	batchCache cmap.ConcurrentMap[string, *pebble.Batch]

	iterLock  sync.Mutex
	iterIdx   uint32
	iterCache cmap.ConcurrentMap[string, *Iter]

	snapLock  sync.Mutex
	snapIdx   uint32
	snapCache cmap.ConcurrentMap[string, *pebble.Snapshot]

	closer     chan func()
	closeEvent chan struct{}
}

func NewServer(host, port, datadir string) (*kvserver, error) {

	var (
		err  error
		size = 2048
	)

	s := &kvserver{
		log: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		batchCache: cmap.New[*pebble.Batch](),
		iterCache:  cmap.New[*Iter](),
		snapCache:  cmap.New[*pebble.Snapshot](),
		addr:       fmt.Sprintf("tcp://%s:%s", host, port),
		closer:     make(chan func(), size),
		buffer:     pbytes.New(8, 4*1024), // 4k
		closeEvent: make(chan struct{}),
	}

	s.wo = pebble.Sync

	// open the database
	db, err := NewPebble(datadir)
	if err != nil {
		return nil, err
	}
	s.db = db

	// init system
	initSystem(s)

	// start the flushdb goroutine
	go s.flushdb()

	// start the handleCloser goroutine
	go s.handleCloser()

	return s, nil
}

func (s *kvserver) flushdb() {
	ticker := time.Tick(time.Minute)
	for range ticker {
		err := s.db.Flush()
		if err != nil {
			s.log.Error("flushdb", "err", err)
		}
	}
}

func (s *kvserver) handleCloser() {

	for fn := range s.closer {
		fn()
	}
	s.closeEvent <- struct{}{}
	s.log.Info("handleCloser exit")
}

func (s *kvserver) Stop(ctx context.Context) {
	s.log.Info("server shutdown and clean all resources...")
	close(s.closer)
	<-s.closeEvent

	cleanSystem(s)
	for _, v := range s.iterCache.Items() {
		v.iter.Close()
	}

	for _, v := range s.batchCache.Items() {
		v.Close()
	}

	for _, v := range s.snapCache.Items() {
		v.Close()
	}

	err := s.db.Close()
	if err != nil {
		s.log.Error("kvserver stop", "err", err)
	}

	s.log.Info("server shutdown success")

	s.eng.Stop(ctx)
}

func (s *kvserver) OnShutdown(c gnet.Engine) {
	return
}

func (s *kvserver) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.log.Info("server booting ...", "addr", s.addr)
	s.eng = eng
	debug.SetMemoryLimit(24 * opt.GiB)
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
