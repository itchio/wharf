package wharf

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
)

var (
	ErrRequestFailure = errors.New("ssh request failed")
)

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
