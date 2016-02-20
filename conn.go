package wharf

import (
	"fmt"
	"net"

	"golang.org/x/crypto/ssh"
)

type Conn struct {
	Conn ssh.Conn

	Chans <-chan ssh.NewChannel
	Reqs  <-chan *ssh.Request

	Permissions *ssh.Permissions

	sessionID string
}

type ErrConnectionRefused struct {
	message string
}

type ErrAuthenticationFailed struct {
	message string
}

func (err *ErrConnectionRefused) Error() string {
	return fmt.Sprintf("connection refused: %s", err.message)
}

func (err *ErrAuthenticationFailed) Error() string {
	return fmt.Sprintf("authentication failed: %s", err.message)
}

// Connect tries to connect to a wharf server
func Connect(address string, privateKeyPath string, client string, version string) (*Conn, error) {
	// I know what you're thinking: the ssh package already accepts
	// an array of AuthMethods right? So surely we don't have to try them
	// one after the other? Well guess again!

	agentMessage := "no ssh agent in sight"
	keyMessage := "no key specified"

	if agentAuth, ok := getAgentAuth(); ok {
		conn, err := tryConnect([]ssh.AuthMethod{agentAuth}, address, client, version)
		if err == nil {
			return conn, nil
		} else {
			if _, ok := err.(*ErrAuthenticationFailed); ok {
				agentMessage = "no usable key in ssh agent"
			} else if _, ok := err.(*ErrConnectionRefused); ok {
				return nil, err
			} else {
				agentMessage = err.Error()
			}
		}
	}

	if privateKeyPath != "" {
		authMethods := make([]ssh.AuthMethod, 0, 1)
		authMethods, err := addKeyAuth(authMethods, privateKeyPath)
		if err == nil {
			conn, err := tryConnect(authMethods, address, client, version)
			if err == nil {
				return conn, nil
			} else {
				if _, ok := err.(*ErrAuthenticationFailed); ok {
					keyMessage = "unknown private key"
				} else {
					keyMessage = err.Error()
				}
			}
		} else {
			keyMessage = err.Error()
		}
	}

	return nil, fmt.Errorf("ssh: unable to authenticate, '%s' and '%s'", agentMessage, keyMessage)
}

func tryConnect(authMethods []ssh.AuthMethod, address string, client string, version string) (*Conn, error) {
	tcpConn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, &ErrConnectionRefused{message: err.Error()}
	}

	sshConfig := &ssh.ClientConfig{
		User:          "wharf",
		Auth:          authMethods,
		ClientVersion: fmt.Sprintf("SSH-2.0-%s_%s", client, version),
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(tcpConn, "", sshConfig)
	if err != nil {
		return nil, &ErrAuthenticationFailed{message: err.Error()}
	}

	return &Conn{
		Conn:  sshConn,
		Chans: chans,
		Reqs:  reqs,
	}, nil
}

func Accept(listener net.Listener, config *ssh.ServerConfig) (*Conn, error) {
	tcpConn, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	sshConn, chans, reqs, err := ssh.NewServerConn(tcpConn, config)
	if err != nil {
		return nil, err
	}

	return &Conn{
		Conn:        sshConn.Conn,
		Permissions: sshConn.Permissions,
		Chans:       chans,
		Reqs:        reqs,
	}, nil
}
