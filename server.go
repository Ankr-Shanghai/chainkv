package main

import (
	"log/slog"
	"os"

	"github.com/Allenxuxu/gev"
	"github.com/cockroachdb/pebble"
)

type kvserver struct {
	server *gev.Server
	db     *pebble.DB
	log    *slog.Logger
}

func NewServer(ip, port, datadir string) (*kvserver, error) {
	var err error
	s := &kvserver{
		log: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
	s.server, err = gev.NewServer(s, gev.Address(ip+":"+port))
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
	err := s.db.Close()
	if err != nil {
		s.log.Error("kvserver stop", "err", err)
	}
	s.server.Stop()
}

func (s *kvserver) OnConnect(c *gev.Connection) {
}

func (s *kvserver) OnMessage(c *gev.Connection, ctx interface{}, data []byte) (out interface{}) {
	return data
}

func (s *kvserver) OnClose(c *gev.Connection) {
}
