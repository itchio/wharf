package pwr_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
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
	t.Logf("== Writing to overlay...")
	_, err := io.Copy(ow, bytes.NewReader(patched))
	assert.NoError(t, err)

	err = ow.Close()
	assert.NoError(t, err)

	t.Logf("== Final overlay size: %s (wrote in %s)", humanize.IBytes(uint64(outbuf.Len())), time.Since(startOverlayTime))
}

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
