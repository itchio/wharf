package wharf

import (
	"encoding/hex"
	"net"
)

func (c *Conn) SessionID() string {
	if c.sessionID == "" {
		c.sessionID = hex.EncodeToString(c.Conn.SessionID())
	}
	return c.sessionID
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Conn) Close() error {
	return c.Conn.Close()
}
