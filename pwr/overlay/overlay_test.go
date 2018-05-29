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

	"github.com/itchio/savior/seeksource"

	"github.com/itchio/httpkit/progress"
	"github.com/itchio/wharf/pwr/overlay"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestOverlayWriterMemory(t *testing.T) {
	roundtripMemory := func(t *testing.T, current []byte, patched []byte) {
		outbuf := new(bytes.Buffer)
		ow, err := overlay.NewOverlayWriter(bytes.NewReader(current), 0, outbuf, 0)
		must(t, err)

		startOverlayTime := time.Now()
		t.Logf("== Writing %s to overlay...", progress.FormatBytes(int64(len(patched))))
		_, err = io.Copy(ow, bytes.NewReader(patched))
		assert.NoError(t, err)

		err = ow.Close()
		assert.NoError(t, err)

		overlaySize := int64(outbuf.Len())
		t.Logf("== Final overlay size: %s (%d bytes) (wrote to memory in %s)", progress.FormatBytes(overlaySize), overlaySize, time.Since(startOverlayTime))

		startPatchTime := time.Now()

		ctx := &overlay.OverlayPatchContext{}
		bws := newBytesWriteSeeker(current, int64(len(patched)))

		patchSource := seeksource.FromBytes(outbuf.Bytes())
		_, err = patchSource.Resume(nil)
		must(t, err)
		err = ctx.Patch(patchSource, bws)
		assert.NoError(t, err)

		patchedSize := int64(len(bws.Bytes()))
		t.Logf("== Final patched size: %s (%d bytes) (applied in memory in %s)", progress.FormatBytes(patchedSize), patchedSize, time.Since(startPatchTime))

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

		intpath := filepath.Join(dir, "intermediate")
		intfile, err := os.OpenFile(intpath, os.O_CREATE|os.O_RDWR, 0644)
		must(t, err)

		defer intfile.Close()
		currentReader := bytes.NewReader(current)
		ow, err := overlay.NewOverlayWriter(currentReader, 0, intfile, 0)
		must(t, err)

		startOverlayTime := time.Now()
		t.Logf("== Writing %s to overlay...", progress.FormatBytes(int64(len(patched))))

		maxBufferSize := 128 * 1024
		refBuf := make([]byte, maxBufferSize)
		prng := rand.New(rand.NewSource(0x39))

		// io.Copy but with random write sizes
		patchedReader := bytes.NewReader(patched)
		for {
			readLen := 1 + prng.Intn(maxBufferSize-1)
			buf := refBuf[:readLen]
			readBytes, readErr := patchedReader.Read(buf)

			wroteBytes, err := ow.Write(buf[:readBytes])
			must(t, err)
			assert.Equal(t, readBytes, wroteBytes)

			if readErr == io.EOF {
				break
			}
			must(t, readErr)

			if prng.Intn(100) > 50 {
				t.Logf("Flushing...")

				// close and redo
				err = ow.Flush()
				must(t, err)

				readOffset := ow.ReadOffset()
				overlayOffset := ow.OverlayOffset()

				t.Logf("Syncing and closing... (readOffset %d, overlayOffset %d)", readOffset, overlayOffset)

				err = intfile.Sync()
				must(t, err)

				err = intfile.Close()
				must(t, err)

				intfile, err = os.OpenFile(intpath, os.O_RDWR, 0644)
				must(t, err)

				_, err = intfile.Seek(overlayOffset, io.SeekStart)
				must(t, err)

				_, err = currentReader.Seek(readOffset, io.SeekStart)
				must(t, err)

				ow, err = overlay.NewOverlayWriter(currentReader, readOffset, intfile, overlayOffset)
				must(t, err)
			}
		}

		err = ow.Close()
		must(t, err)

		err = intfile.Sync()
		must(t, err)

		overlayStats, err := intfile.Stat()
		must(t, err)

		overlaySize := overlayStats.Size()

		t.Logf("== Final overlay size: %s (%d bytes) (wrote to fs in %s)", progress.FormatBytes(overlaySize), overlaySize, time.Since(startOverlayTime))

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
		patchSource := seeksource.FromFile(intfile)
		_, err = patchSource.Resume(nil)
		must(t, err)
		err = ctx.Patch(patchSource, patchedfile)
		must(t, err)

		patchedSize, err := patchedfile.Seek(0, io.SeekCurrent)
		must(t, err)

		t.Logf("== Final patched size: %s (%d bytes) (applied to fs in %s)", progress.FormatBytes(patchedSize), patchedSize, time.Since(startPatchTime))

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

	t.Logf("Generating %s of random data...", progress.FormatBytes(fullDataSize))
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

		for j := 0; j < freshSize; j++ {
			patched[freshPosition+j] = byte(rng.Intn(256))
		}
	}
	t.Logf("Testing slightly-different data...")
	tester(t, current, patched)

	t.Logf("Testing larger data...")
	{
		trailingSize := 1024 * (256 + rng.Intn(256))

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
