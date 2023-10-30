package pool

import (
	"net"
	"sync"
)

type Conn struct {
	net.Conn
	mu sync.RWMutex
	c  *channelPool
}

func (c *Conn) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.c.put(c.Conn)
}
