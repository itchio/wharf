package overlay_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/itchio/wharf/pwr/overlay"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestOverlayWriterMemory(t *testing.T) {
	roundtripMemory := func(t *testing.T, current []byte, patched []byte) {
		outbuf := new(bytes.Buffer)
		ow := overlay.NewOverlayWriter(bytes.NewReader(current), outbuf)

		startOverlayTime := time.Now()
		t.Logf("== Writing %s to overlay...", humanize.IBytes(uint64(len(patched))))
		_, err := io.Copy(ow, bytes.NewReader(patched))
		assert.NoError(t, err)

		err = ow.Close()
		assert.NoError(t, err)

		t.Logf("== Final overlay size: %s (wrote to memory in %s)", humanize.IBytes(uint64(outbuf.Len())), time.Since(startOverlayTime))

		startPatchTime := time.Now()

		ctx := &overlay.OverlayPatchContext{}
		bws := newBytesWriteSeeker(current, int64(len(patched)))

		err = ctx.Patch(bytes.NewReader(outbuf.Bytes()), bws)
		assert.NoError(t, err)

		t.Logf("== Final patched size: %s (applied in memory in %s)", humanize.IBytes(uint64(len(bws.Bytes()))), time.Since(startPatchTime))

		assert.EqualValues(t, len(patched), len(bws.Bytes()))
		assert.EqualValues(t, patched, bws.Bytes())
	}

	testOverlayWriter(t, roundtripMemory)
}

func TestOverlayWriterFS(t *testing.T) {
	dir, err := ioutil.TempDir("", "overlay")
	must(t, err)

	t.Logf("Using temp dir %s", dir)
	if !(os.Getenv("OVERLAY_KEEP_DIR") == "1") {
		defer os.RemoveAll(dir)
	}

	roundtripFs := func(t *testing.T, current []byte, patched []byte) {
		must(t, os.RemoveAll(dir))
		must(t, os.MkdirAll(dir, 0755))

		intfile, err := os.Create(filepath.Join(dir, "intermediate"))
		must(t, err)

		defer intfile.Close()
		ow := overlay.NewOverlayWriter(bytes.NewReader(current), intfile)

		startOverlayTime := time.Now()
		t.Logf("== Writing %s to overlay...", humanize.IBytes(uint64(len(patched))))
		_, err = io.Copy(ow, bytes.NewReader(patched))
		assert.NoError(t, err)

		err = ow.Close()
		assert.NoError(t, err)

		overlaySize, err := intfile.Seek(0, io.SeekCurrent)
		must(t, err)

		err = intfile.Sync()
		must(t, err)

		t.Logf("== Final overlay size: %s (wrote to fs in %s)", humanize.IBytes(uint64(overlaySize)), time.Since(startOverlayTime))

		// now rewind
		_, err = intfile.Seek(0, io.SeekStart)
		must(t, err)

		patchedfile, err := os.Create(filepath.Join(dir, "patched"))
		must(t, err)

		defer patchedfile.Close()

		// make it look like the current file
		_, err = patchedfile.Write(current)
		must(t, err)

		// then rewind
		_, err = patchedfile.Seek(0, io.SeekStart)
		must(t, err)

		startPatchTime := time.Now()

		err = patchedfile.Truncate(int64(len(patched)))
		must(t, err)

		ctx := &overlay.OverlayPatchContext{}
		err = ctx.Patch(intfile, patchedfile)
		assert.NoError(t, err)

		patchedSize, err := patchedfile.Seek(0, io.SeekCurrent)
		must(t, err)

		t.Logf("== Final patched size: %s (applied to fs in %s)", humanize.IBytes(uint64(patchedSize)), time.Since(startPatchTime))

		_, err = patchedfile.Seek(0, io.SeekStart)
		must(t, err)

		result, err := ioutil.ReadAll(patchedfile)
		must(t, err)

		assert.EqualValues(t, len(patched), len(result))
		assert.EqualValues(t, patched, result)
	}

	defer testOverlayWriter(t, roundtripFs)
}

func must(t *testing.T, err error) {
	if err != nil {
		assert.NoError(t, err)
		t.FailNow()
	}
}

type testerFunc func(t *testing.T, current []byte, patched []byte)

func testOverlayWriter(t *testing.T, tester testerFunc) {
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
	tester(t, current, patched)

	t.Logf("Testing pristine data...")
	copy(patched, current)
	tester(t, current, patched)

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
	tester(t, current, patched)

	t.Logf("Testing larger data...")
	{
		trailingSize := 1024 * (256 + rng.Intn(256))
		// t.Logf("Adding %s trailing data", humanize.IBytes(uint64(trailingSize)))

		patched = append(patched, patched[:trailingSize]...)
	}
	tester(t, current, patched)

	t.Logf("Testing smaller data...")
	patched = patched[:fullDataSize/2]
	tester(t, current, patched)
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
