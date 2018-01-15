package pwr

import (
	"bytes"
	"io"
	"testing"

	"github.com/itchio/savior"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/wire"
	"github.com/stretchr/testify/assert"
)

type fakeCompressor struct {
	called  bool
	quality int32
}

var _ Compressor = (*fakeCompressor)(nil)

func (fc *fakeCompressor) Apply(writer io.Writer, quality int32) (io.Writer, error) {
	fc.called = true
	fc.quality = quality
	return writer, nil
}

type fakeDecompressor struct {
	called bool
}

var _ Decompressor = (*fakeDecompressor)(nil)

func (fd *fakeDecompressor) Apply(source savior.Source) (savior.Source, error) {
	fd.called = true
	return source, nil
}

func Test_Compression(t *testing.T) {
	fc := &fakeCompressor{}
	RegisterCompressor(CompressionAlgorithm_GZIP, fc)

	fd := &fakeDecompressor{}
	RegisterDecompressor(CompressionAlgorithm_GZIP, fd)

	assert.EqualValues(t, false, fc.called)

	buf := new(bytes.Buffer)
	wc := wire.NewWriteContext(buf)
	_, err := CompressWire(wc, &CompressionSettings{
		Algorithm: CompressionAlgorithm_BROTLI,
		Quality:   3,
	})
	assert.NotNil(t, err)

	cwc, err := CompressWire(wc, &CompressionSettings{
		Algorithm: CompressionAlgorithm_GZIP,
		Quality:   3,
	})
	assert.NoError(t, err)

	assert.EqualValues(t, true, fc.called)
	assert.EqualValues(t, 3, fc.quality)

	assert.NoError(t, cwc.WriteMessage(&SyncHeader{
		FileIndex: 672,
	}))

	ss := seeksource.FromBytes(buf.Bytes())
	_, err = ss.Resume(nil)
	assert.NoError(t, err)

	rc := wire.NewReadContext(ss)

	sh := &SyncHeader{}
	assert.NoError(t, rc.ReadMessage(sh))

	assert.EqualValues(t, 672, sh.FileIndex)
	assert.NotNil(t, rc.ReadMessage(sh))
}
