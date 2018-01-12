package pwr_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pwr"
	"github.com/stretchr/testify/assert"
)

func TestOverlayWriter(t *testing.T) {
	const fullDataSize = 4 * 1024 * 1024
	current := make([]byte, fullDataSize)
	patched := make([]byte, fullDataSize)

	t.Logf("Generating %s of random data...", humanize.IBytes(uint64(fullDataSize)))
	startGenTime := time.Now()

	rng := rand.New(rand.NewSource(0xf891))

	for i := 0; i < fullDataSize; i++ {
		current[i] = byte(rng.Intn(256))
	}

	t.Logf("Generated in %s", time.Since(startGenTime))

	t.Logf("Testing null-byte data...")
	testOverlayRoundtrip(t, current, patched)

	t.Logf("Testing pristine data...")
	copy(patched, current)
	testOverlayRoundtrip(t, current, patched)

	for i := 0; i < 16; i++ {
		freshSize := 1024 * rng.Intn(256)
		freshPosition := rng.Intn(fullDataSize)

		if freshPosition+freshSize > fullDataSize {
			freshSize = fullDataSize - freshPosition
		}
		// t.Logf("Adding %s disturbance at %d", humanize.IBytes(uint64(freshSize)), freshPosition)

		for j := 0; j < freshSize; j++ {
			patched[freshPosition+j] = byte(rng.Intn(256))
		}
	}
	t.Logf("Testing slightly-different data...")
	testOverlayRoundtrip(t, current, patched)

	t.Logf("Testing larger data...")
	{
		trailingSize := 1024 * (256 + rng.Intn(256))
		// t.Logf("Adding %s trailing data", humanize.IBytes(uint64(trailingSize)))

		patched = append(patched, patched[:trailingSize]...)
	}
	testOverlayRoundtrip(t, current, patched)

	t.Logf("Testing smaller data...")
	patched = patched[:fullDataSize/2]
	testOverlayRoundtrip(t, current, patched)
}

func testOverlayRoundtrip(t *testing.T, current []byte, patched []byte) {
	outbuf := new(bytes.Buffer)
	ow := pwr.NewOverlayWriter(bytes.NewReader(current), &nopCloserWriter{outbuf})

	startOverlayTime := time.Now()
	t.Logf("== Writing %s to overlay...", humanize.IBytes(uint64(len(patched))))
	_, err := io.Copy(ow, bytes.NewReader(patched))
	assert.NoError(t, err)

	err = ow.Close()
	assert.NoError(t, err)

	t.Logf("== Final overlay size: %s (wrote in %s)", humanize.IBytes(uint64(outbuf.Len())), time.Since(startOverlayTime))

	startPatchTime := time.Now()

	ctx := &pwr.OverlayPatchContext{}
	bws := newBytesWriteSeeker(current, int64(len(patched)))

	err = ctx.Patch(bytes.NewReader(outbuf.Bytes()), bws)
	assert.NoError(t, err)

	t.Logf("== Final patched size: %s (applied in %s)", humanize.IBytes(uint64(len(bws.Bytes()))), time.Since(startPatchTime))

	assert.EqualValues(t, len(patched), len(bws.Bytes()))
	assert.EqualValues(t, patched, bws.Bytes())
}

// bytesWriteSeeker

type bytesWriteSeeker struct {
	b []byte

	size   int64
	offset int64
}

var _ io.WriteSeeker = (*bytesWriteSeeker)(nil)

func newBytesWriteSeeker(current []byte, size int64) *bytesWriteSeeker {
	b := make([]byte, size)
	copy(b, current)

	return &bytesWriteSeeker{
		b:      b,
		size:   size,
		offset: 0,
	}
}

func (bws *bytesWriteSeeker) Write(buf []byte) (int, error) {
	copiedLen := copy(bws.b[int(bws.offset):], buf)
	bws.offset += int64(copiedLen)
	return copiedLen, nil
}

func (bws *bytesWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		bws.offset = offset
	case io.SeekCurrent:
		bws.offset += offset
	case io.SeekEnd:
		bws.offset = bws.size + offset
	default:
		return bws.offset, errors.New("invalid whence")
	}

	if bws.offset < 0 || bws.offset > bws.size {
		return bws.offset, errors.New("invalid seek offset")
	}

	return bws.offset, nil
}

func (bws *bytesWriteSeeker) Bytes() []byte {
	return bws.b
}

// nopCloserWriter

type nopCloserWriter struct {
	writer io.Writer
}

var _ io.WriteCloser = (*nopCloserWriter)(nil)

func (ncw *nopCloserWriter) Write(buf []byte) (int, error) {
	return ncw.writer.Write(buf)
}

func (ncw *nopCloserWriter) Close() error {
	return nil
}
