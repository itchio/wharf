package pwr

import (
	"fmt"

	"github.com/itchio/wharf/wdec"
	"github.com/itchio/wharf/wire"
	"gopkg.in/kothar/brotli-go.v0/enc"
)

// CompressionDefault returns default compression settings for wharf-based software
// Brotli Q1 is, overall, slightly slower than gzip, a lot faster than lzma,
// and compresses somewhere between the two. it's ideal for transferring.
func CompressionDefault() *CompressionSettings {
	return &CompressionSettings{
		Algorithm: CompressionAlgorithm_BROTLI,
		Quality:   1,
	}
}

// ToString returns a human-readable description of given compression settings
func (cs *CompressionSettings) ToString() string {
	switch cs.Algorithm {
	case CompressionAlgorithm_UNCOMPRESSED:
		return "none"
	case CompressionAlgorithm_BROTLI:
		return fmt.Sprintf("brotli-q%d", cs.Quality)
	default:
		return "unknown"
	}
}

// CompressWire wraps a wire.WriteContext into a compressor, according to given settings,
// so that any messages written through the returned WriteContext will first be compressed.
func CompressWire(ctx *wire.WriteContext, compression *CompressionSettings) (*wire.WriteContext, error) {
	if compression == nil {
		compression = CompressionDefault()
	}

	switch compression.Algorithm {
	case CompressionAlgorithm_UNCOMPRESSED:
		return ctx, nil
	case CompressionAlgorithm_BROTLI:
		params := enc.NewBrotliParams()
		params.SetQuality(int(compression.Quality))
		brotliWriter := enc.NewBrotliWriter(params, ctx.Writer())
		return wire.NewWriteContext(brotliWriter), nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %v", compression.Algorithm)
	}
}

// UncompressWire wraps a wire.ReadContext into a decompressor, according to the given settings,
// so that any messages read through the returned ReadContext will first be decompressed.
func UncompressWire(ctx *wire.ReadContext, compression *CompressionSettings) (*wire.ReadContext, error) {
	switch compression.Algorithm {
	case CompressionAlgorithm_UNCOMPRESSED:
		return ctx, nil
	case CompressionAlgorithm_BROTLI:
		// TODO: revert to canonical brotli-go fork
		brotliReader := wdec.NewBrotliReader(ctx.Reader())
		return wire.NewReadContext(brotliReader), nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %v", compression.Algorithm)
	}
}
