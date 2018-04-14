package ctxcopy_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/itchio/wharf/ctxcopy"
	"github.com/itchio/wharf/werrors"
	"github.com/stretchr/testify/assert"
)

func Test_CtxCopy(t *testing.T) {
	buf := make([]byte, 4*1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	r := bytes.NewReader(buf)
	w := new(bytes.Buffer)

	n, err := ctxcopy.Do(context.Background(), w, r)
	assert.NoError(t, err)
	assert.EqualValues(t, len(buf), n)
}

func Test_CtxCopyCancel(t *testing.T) {
	buf := make([]byte, 4*1024*1024)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}

	r := bytes.NewReader(buf)
	w := new(bytes.Buffer)

	ctx, cancel := context.WithCancel(context.Background())

	cr := &cancelReader{
		upstream:  r,
		threshold: 2 * 1024 * 1024,
		cancel:    cancel,
	}

	n, err := ctxcopy.Do(ctx, w, cr)
	assert.Error(t, err)
	assert.Equal(t, werrors.ErrCancelled, err)
	assert.True(t, n > 1*1024*1024)
	assert.True(t, n < 3*1024*1024)
}

type cancelReader struct {
	upstream  io.Reader
	threshold int64
	cancel    context.CancelFunc

	count int64
}

var _ io.Reader = (*cancelReader)(nil)

func (cr *cancelReader) Read(buf []byte) (int, error) {
	if cr.count > cr.threshold {
		cr.cancel()
	}

	n, err := cr.upstream.Read(buf)
	cr.count += int64(n)
	return n, err
}
