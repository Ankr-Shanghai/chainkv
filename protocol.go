package main

import (
	"encoding/binary"

	"github.com/Allenxuxu/gev"
	"github.com/Allenxuxu/ringbuffer"
	"github.com/gobwas/pool/pbytes"
)

type Protocol struct {
}

func (p *Protocol) UnPacket(c *gev.Connection, buffer *ringbuffer.RingBuffer) (ctx interface{}, out []byte) {
	if buffer.Length() > 6 {
		length := int(buffer.PeekUint32())
		if buffer.Length() >= length+4 {
			buffer.Retrieve(4)

			typeLen := int(buffer.PeekUint16())
			buffer.Retrieve(2)

			typeByte := pbytes.GetLen(typeLen)
			_, _ = buffer.Read(typeByte)

			dataLen := length - 2 - typeLen
			data := make([]byte, dataLen)
			_, _ = buffer.Read(data)

			out = data
			ctx = string(typeByte)
			pbytes.Put(typeByte)
		}
	}

	return
}

func (p *Protocol) Packet(c *gev.Connection, data interface{}) []byte {
	ds := data.([]byte)
	dslen := pbytes.GetLen(4)
	defer func() {
		pbytes.Put(dslen)
		ds = nil
	}()
	ret := make([]byte, 0, 4+len(ds))
	binary.BigEndian.PutUint32(dslen, uint32(len(ds)))

	return append(append(ret, dslen...), ds...)
}
