package pool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Pool interface {
	Get() (net.Conn, error)
	Len() int
	Close()
}

type Factory func() (net.Conn, error)
type Check func(net.Conn) error

type channelPool struct {
	mu      sync.RWMutex
	conns   chan net.Conn
	factory Factory
	status  Check
}

func NewPool(initSize, capSize int, factory Factory, check Check) (Pool, error) {
	if initSize < 0 || capSize <= 0 || initSize > capSize {
		return nil, errors.New("invalid size parameters")
	}
	c := &channelPool{
		conns:   make(chan net.Conn, capSize),
		factory: factory,
		status:  check,
	}

	for i := 0; i < initSize; i++ {
		conn, err := factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- conn
	}

	if c.status != nil {
		go c.checkConn()
	}

	return c, nil
}

func (c *channelPool) checkConn() error {
	tick := time.Tick(45 * time.Second)
	for range tick {
		for conn := range c.conns {
			err := c.status(conn)
			if err != nil {
				conn.Close()
				continue
			}
			c.conns <- conn
		}
	}
	return nil
}

func (c *channelPool) getConnsAndFactory() (chan net.Conn, Factory) {
	c.mu.RLock()
	conns := c.conns
	factory := c.factory
	c.mu.RUnlock()
	return conns, factory
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
	}
}

var ErrClosed = errors.New("pool is closed")

func (c *channelPool) Get() (net.Conn, error) {
	conns, factory := c.getConnsAndFactory()
	if conns == nil {
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		return c.wrapConn(conn), nil
	default:
		conn, err := factory()
		if err != nil {
			return nil, err
		}
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

		return c.wrapConn(conn), nil
	}
}

// newConn wraps a standard net.Conn to a poolConn net.Conn.
func (c *channelPool) wrapConn(conn net.Conn) net.Conn {
	p := &Conn{c: c}
	p.Conn = conn
	return p
}

func (c *channelPool) Len() int {
	return len(c.conns)
}

func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- conn:
		return nil
	default:
		// pool is full, close passed connection
		return conn.Close()
	}
}
