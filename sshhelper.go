/*

Substantial code from rtop - the remote system monitoring utility

Copyright (c) 2015 RapidLoop
Copyright (c) 2016 Amos Wenger

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package wharf

import (
	"bufio"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	// ErrPasswordInputAborted is thrown when the user cancels (Ctrl+C) during password input
	ErrPasswordInputAborted = errors.New("Password input aborted")
)

// unconvert will complain about this because syscall.Stdin actually is an int
// on unixy platforms, but not on Win32 (where it's a HANDLE). That API is
// just badly designed, we could work around it with build constraints but
// for now let's let the linter have its whinery.
var stdin = int(syscall.Stdin)

func getpass(prompt string) (string, error) {
	var err error

	tstate, err := terminal.GetState(stdin)
	if err != nil {
		return "", fmt.Errorf("in terminal.GetState(): %s", err.Error())
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		quit := false
		for _ = range sig {
			quit = true
			break
		}
		err := terminal.Restore(stdin, tstate)
		if err != nil {
			err = fmt.Errorf("in terminal.Restore(): %s", err.Error())
		}

		if quit {
			fmt.Println()
			err = ErrPasswordInputAborted
		}
	}()
	defer func() {
		signal.Stop(sig)
		close(sig)
	}()

	f := bufio.NewWriter(os.Stdout)
	_, wErr := f.Write([]byte(prompt))
	if wErr != nil {
		return "", wErr
	}
	wErr = f.Flush()
	if wErr != nil {
		return "", wErr
	}

	passbytes, err := terminal.ReadPassword(stdin)
	if err != nil {
		err = fmt.Errorf("in terminal.ReadPassword(): %s", err.Error())
	}
	pass := string(passbytes)

	_, wErr = f.Write([]byte("\n"))
	if wErr != nil {
		return "", wErr
	}
	wErr = f.Flush()
	if wErr != nil {
		return "", wErr
	}

	return pass, err
}

// ParsePemBlock parses a private key block in RSA, EC, or DSA format
// ref golang.org/x/crypto/ssh/keys.go#ParseRawPrivateKey
func ParsePemBlock(block *pem.Block) (interface{}, error) {
	switch block.Type {
	case "RSA PRIVATE KEY":
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(block.Bytes)
	case "DSA PRIVATE KEY":
		return ssh.ParseDSAPrivateKey(block.Bytes)
	default:
		return nil, fmt.Errorf("rtop: unsupported key type %q", block.Type)
	}
}

func addKeyAuth(auths []ssh.AuthMethod, keypath string) ([]ssh.AuthMethod, error) {
	if keypath == "" {
		return auths, nil
	}

	// read the file
	pemBytes, err := ioutil.ReadFile(keypath)
	if err != nil {
		return auths, err
	}

	// get first pem block
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		log.Printf("no key found in %s", keypath)
		return auths, nil
	}

	// handle encrypted keyfiles
	if x509.IsEncryptedPEMBlock(block) {
		prompt := fmt.Sprintf("Enter passphrase for key '%s': ", keypath)
		var pass string
		pass, err = getpass(prompt)
		if err != nil {
			return auths, err
		}
		block.Bytes, err = x509.DecryptPEMBlock(block, []byte(pass))
		if err != nil {
			return auths, err
		}
		var key interface{}
		key, err = ParsePemBlock(block)
		if err != nil {
			return auths, err
		}
		var signer ssh.Signer
		signer, err = ssh.NewSignerFromKey(key)
		if err != nil {
			return auths, err
		}
		return append(auths, ssh.PublicKeys(signer)), nil
	}

	// handle plain keyfiles
	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return auths, err
	}
	return append(auths, ssh.PublicKeys(signer)), nil
}

func getAgentAuth() (auth ssh.AuthMethod, ok bool) {
	if sock := os.Getenv("SSH_AUTH_SOCK"); len(sock) > 0 {
		if agconn, err := net.Dial("unix", sock); err == nil {
			ag := agent.NewClient(agconn)
			auth = ssh.PublicKeysCallback(ag.Signers)
			ok = true
		}
	}
	return
}
