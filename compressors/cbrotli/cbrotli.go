package cbrotli

import (
	"github.com/andybalholm/brotli"
	"github.com/itchio/wharf/pwr"
	"io"
)

type Writer struct{}

func (bc *Writer) Apply(writer io.Writer, quality int32) (io.Writer, error) {
	return brotli.NewWriterLevel(writer, int(quality)), nil
}

func init() {
	pwr.RegisterCompressor(pwr.CompressionAlgorithm_BROTLI, &Writer{})
}
