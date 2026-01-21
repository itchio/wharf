package multiread_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/itchio/wharf/multiread"
	"github.com/stretchr/testify/assert"
)

func Test_Simple(t *testing.T) {
	buf := []byte("Hello world")

	mr := multiread.New(bytes.NewReader(buf))
	r1 := mr.Reader()
	r2 := mr.Reader()
	r3 := mr.Reader()

	var b1, b2, b3 []byte

	done := make(chan error)
	go func() { done <- mr.Do(context.Background()) }()

	time.Sleep(200 * time.Millisecond)

	r4 := mr.Reader()
	_, err := r4.Read([]byte{0x0})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot call Reader()")

	go func() { var err error; b1, err = io.ReadAll(r1); done <- err }()
	go func() { var err error; b2, err = io.ReadAll(r2); done <- err }()
	go func() { var err error; b3, err = io.ReadAll(r3); done <- err }()

	for i := 0; i < 4; i++ {
		assert.NoError(t, <-done)
	}
	assert.EqualValues(t, buf, b1)
	assert.EqualValues(t, buf, b2)
	assert.EqualValues(t, buf, b3)
}

func Test_Error(t *testing.T) {
	r := io.MultiReader(strings.NewReader("Tonight at 11: "), &errorReader{})

	mr := multiread.New(r)
	r1 := mr.Reader()
	r2 := mr.Reader()
	r3 := mr.Reader()

	doneMR := make(chan error)
	done1 := make(chan error)
	done2 := make(chan error)
	done3 := make(chan error)

	go func() { doneMR <- mr.Do(context.Background()) }()
	go func() { _, err := io.ReadAll(r1); done1 <- err }()
	go func() { _, err := io.ReadAll(r2); done2 <- err }()
	go func() { _, err := io.ReadAll(r3); done3 <- err }()

	drain := func(c chan error) {
		t.Helper()
		err := <-c
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "DOOM!")
	}
	drain(doneMR)
	drain(done1)
	drain(done2)
	drain(done3)
}

type errorReader struct{}

func (er *errorReader) Read(p []byte) (int, error) {
	return 0, errors.New("DOOM!")
}
