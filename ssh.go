package wharf

import (
	"encoding/hex"
	"net"
)

// SessionID returns a human-readable version of the SSH session identifier
func (c *Conn) SessionID() string {
	if c.sessionID == "" {
		c.sessionID = hex.EncodeToString(c.Conn.SessionID())
	}
	return c.sessionID
}

// RemoteAddr returns the remote address of a wharf connection
func (c *Conn) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

// Close closes a wharf connection
func (c *Conn) Close() error {
	return c.Conn.Close()
}
