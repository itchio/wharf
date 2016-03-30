package wharf

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
)

var (
	// ErrRequestFailure is thrown when there is an SSH-level problem with sending a request
	ErrRequestFailure = errors.New("ssh request failed")
)

// SendRequest sends a protobuf message over a wharf connection, and reads a reply
// if a non-nil reply is supplied
func (c *Conn) SendRequest(name string, request proto.Message, reply proto.Message) error {
	wantReply := reply != nil

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		return err
	}

	status, replyBytes, err := c.Conn.SendRequest(name, wantReply, requestBytes)
	if err != nil {
		err = fmt.Errorf("in SendRequest(%s): %s", name, err.Error())
		return err
	}

	if !status {
		return ErrRequestFailure
	}

	if wantReply {
		err = proto.Unmarshal(replyBytes, reply)
		if err != nil {
			return err
		}
	}

	return nil
}
