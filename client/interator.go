package client

import (
	"errors"

	"github.com/Ankr-Shanghai/chainkv/retcode"
	"github.com/Ankr-Shanghai/chainkv/types"
)

type Iterator struct {
	client *client
	idx    uint32
}

func (i *Iterator) Next() bool {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_NEXT,
			Id:   i.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := i.client.do(req, rsp)
	if err != nil {
		return false
	}

	return rsp.Exist
}

func (i *Iterator) Key() []byte {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_KEY,
			Id:   i.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := i.client.do(req, rsp)
	if err != nil {
		return nil
	}

	return rsp.Val
}

func (i *Iterator) Value() []byte {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_VAL,
			Id:   i.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := i.client.do(req, rsp)
	if err != nil {
		return nil
	}

	return rsp.Val
}

func (i *Iterator) Error() error {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_ERROR,
			Id:   i.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := i.client.do(req, rsp)
	if err != nil {
		return err
	}

	if rsp.Exist {
		return errors.New("iterator error")
	}

	return nil
}

func (i *Iterator) Close() error {
	var (
		req = &types.Request{
			Type: types.REQ_TYPE_ITER_CLOSE,
			Id:   i.idx,
		}
		rsp = &types.Response{Code: retcode.CodeOK}
	)

	err := i.client.do(req, rsp)
	if err != nil {
		return err
	}

	// should remove from iterMap
	i.client.iterLock.Lock()
	delete(i.client.iterMap, i.idx)
	i.client.iterLock.Unlock()

	return nil
}
