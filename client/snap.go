package client

import (
	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

type Snap struct {
	client *client
	idx    uint32
}

func (s *Snap) Get(key []byte) ([]byte, error) {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_SNAP_GET,
			Id:   s.idx,
			Key:  key,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return nil, err
	}

	return rsp.Val, nil
}

func (s *Snap) Has(key []byte) (bool, error) {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_SNAP_HAS,
			Id:   s.idx,
			Key:  key,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return false, err
	}

	return rsp.Exist, nil
}

func (s *Snap) Release() error {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_SNAP_RELEASE,
			Id:   s.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := s.client.do(req, rsp)
	if err != nil {
		return err
	}

	s.client.snapMap.Del(s.idx)

	return nil
}
