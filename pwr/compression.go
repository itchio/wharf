package pwr

import (
	"fmt"

	"github.com/itchio/wharf/wire"
	"gopkg.in/kothar/brotli-go.v0/dec"
	"gopkg.in/kothar/brotli-go.v0/enc"
)

func CompressionDefault() *CompressionSettings {
	// brotli Q1 is, overall, slightly slower than gzip, a lot faster than lzma,
	// and compresses somewhere between the two. it's ideal for transferring.
	return &CompressionSettings{
		Algorithm: CompressionAlgorithm_BROTLI,
		Quality:   1,
	}
}

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

func UncompressWire(ctx *wire.ReadContext, compression *CompressionSettings) (*wire.ReadContext, error) {
	switch compression.Algorithm {
	case CompressionAlgorithm_UNCOMPRESSED:
		return ctx, nil
	case CompressionAlgorithm_BROTLI:
		brotliReader := dec.NewBrotliReader(ctx.Reader())
		return wire.NewReadContext(brotliReader), nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %v", compression.Algorithm)
	}
}
