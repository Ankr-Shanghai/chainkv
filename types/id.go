package types

import "strconv"

type ID uint32

func (id ID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

func (id ID) UInt32() uint32 {
	return uint32(id)
}
