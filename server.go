package main

import (
	"log/slog"
	"os"
	"runtime"
	"sync"

	"github.com/Allenxuxu/gev"
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"
)

type kvserver struct {
	server *gev.Server
	db     *pebble.DB
	log    *slog.Logger

	lock      sync.Mutex
	connTotal int

	batchLock  sync.Mutex
	batchIdx   uint32
	batchCache map[uint32]*pebble.Batch

	iterLock  sync.Mutex
	iterIdx   uint32
	iterCache map[uint32]*Iter

	snapLock  sync.Mutex
	snapIdx   uint32
	snapCache map[uint32]*pebble.Snapshot
}

func NewServer(ip, port, datadir string) (*kvserver, error) {

	var err error
	s := &kvserver{
		log: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
		batchCache: make(map[uint32]*pebble.Batch),
		iterCache:  make(map[uint32]*Iter),
		snapCache:  make(map[uint32]*pebble.Snapshot),
	}

	s.server, err = gev.NewServer(s, gev.Address(ip+":"+port),
		gev.CustomProtocol(&Protocol{}),
		gev.NumLoops(runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	// open the database
	db, err := NewPebble(datadir)
	if err != nil {
		return nil, err
	}
	s.db = db

	return s, nil
}

func (s *kvserver) Start() {
	s.server.Start()
}

func (s *kvserver) Stop() {
	err := s.db.Flush()
	if err != nil {
		s.log.Error("kvserver flush memtable to stable storage", "err", err)
	}
	err = s.db.Close()
	if err != nil {
		s.log.Error("kvserver stop", "err", err)
	}

	s.batchLock.Lock()
	for _, batch := range s.batchCache {
		batch.Close()
	}
	s.batchLock.Unlock()

	s.iterLock.Lock()
	for _, iter := range s.iterCache {
		iter.iter.Close()
	}
	s.iterLock.Unlock()

	s.server.Stop()
}

func (s *kvserver) OnConnect(c *gev.Connection) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.connTotal++
	s.log.Info("OnConnect", "connTotal", s.connTotal, "remoteAddr", c.PeerAddr())
}

func (s *kvserver) OnMessage(c *gev.Connection, ctx interface{}, data []byte) (out interface{}) {
	name := ctx.(string)
	s.log.Debug("OnMessage", "name", name, "data", data)
	handler, ok := handleOps[name]
	if !ok {
		rsp := &pb.NotSupport{Code: retcode.ErrNotSupport}
		out, _ = proto.Marshal(rsp)
		return
	}
	out = handler(s, data)
	return
}

func (s *kvserver) OnClose(c *gev.Connection) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.connTotal--
	s.log.Info("OnClose", "connTotal", s.connTotal, "remoteAddr", c.PeerAddr())
}
