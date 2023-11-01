package client

import (
	"github.com/Ankr-Shanghai/chainkv/client/pb"
	"github.com/Ankr-Shanghai/chainkv/retcode"
)

type Snap struct {
	client *client
	idx    uint32
}

func (s *Snap) Get(key []byte) ([]byte, error) {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_SNAP_GET,
			Id:   s.idx,
			Key:  key,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return nil, err
	}

	return rsp.Val, nil
}

func (s *Snap) Has(key []byte) (bool, error) {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_SNAP_HAS,
			Id:   s.idx,
			Key:  key,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return false, err
	}

	return rsp.Exist, nil
}

func (s *Snap) Release() error {
	var (
		req = &pb.Request{
			Type: pb.ReqType_REQ_TYPE_SNAP_RELEASE,
			Id:   s.idx,
		}
		rsp = &pb.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return err
	}

	s.client.snapLock.Lock()
	delete(s.client.snapMap, s.idx)
	s.client.snapLock.Unlock()

	return nil
}