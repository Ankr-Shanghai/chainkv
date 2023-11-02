package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/codec"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/panjf2000/gnet/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/protobuf/proto"
)

type kvserver struct {
	gnet.BuiltinEventEngine
	eng  gnet.Engine
	log  *slog.Logger
	addr string

	db *leveldb.DB
	wo *opt.WriteOptions

	batchLock  sync.Mutex
	batchIdx   uint32
	batchCache map[uint32]*leveldb.Batch

	iterLock  sync.Mutex
	iterIdx   uint32
	iterCache map[uint32]*Iter

	snapLock  sync.Mutex
	snapIdx   uint32
	snapCache map[uint32]*leveldb.Snapshot
}

func NewServer(host, port, datadir string) (*kvserver, error) {

	var err error
	s := &kvserver{
		log: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		batchCache: make(map[uint32]*leveldb.Batch),
		iterCache:  make(map[uint32]*Iter),
		snapCache:  make(map[uint32]*leveldb.Snapshot),
		addr:       fmt.Sprintf("tcp://%s:%s", host, port),
	}

	s.wo = &opt.WriteOptions{
		Sync: true,
	}

	// open the database
	db, err := NewDatabase(datadir)
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
	s.iterLock.Lock()
	for _, iter := range s.iterCache {
		iter.iter.Release()
	}
	s.iterLock.Unlock()

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
	req := &pb.Request{}
	err = proto.Unmarshal(data, req)
	if err != nil {
		s.log.Error("OnTraffic unmarshal", "err", err)
		return
	}

	handler, ok := handleOps[req.Type]
	if !ok {
		rsp := &pb.NotSupport{Code: retcode.ErrNotSupport}
		data, _ = proto.Marshal(rsp)
		return
	}
	rsp := handler(s, req)
	rs, _ := proto.Marshal(rsp)
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
