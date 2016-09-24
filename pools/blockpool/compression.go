package blockpool

import (
	"github.com/Datadog/zstd"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/tlc"
)

/////////////////////////////
// Sink
/////////////////////////////

// A CompressingSink compresses blocks with ztsd-q9 before storing them
// to the underlying sink
type CompressingSink struct {
	Sink Sink

	compressedBuf []byte
}

var _ Sink = (*CompressingSink)(nil)

// Store first compresses the data, then stores it into the underling sink
func (cs *CompressingSink) Store(loc BlockLocation, data []byte) error {
	if cs.compressedBuf == nil {
		// planning for the worst case scenario - that compressing the data
		// actually increased the block size
		cs.compressedBuf = make([]byte, BigBlockSize*2)
	}

	_, err := zstd.CompressLevel(cs.compressedBuf, data, 9)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	return cs.Sink.Store(loc, cs.compressedBuf)
}

// GetContainer returns the underlying source's container
func (cs *CompressingSink) GetContainer() *tlc.Container {
	return cs.Sink.GetContainer()
}

// Clone returns a copy of this decomressing source
func (cs *CompressingSink) Clone() Sink {
	return &CompressingSink{
		Sink: cs.Sink.Clone(),
	}
}

/////////////////////////////
// Source
/////////////////////////////

// A DecompressingSource decompresses zstd-compressed blocks before
// when fetching them from the underlying source
type DecompressingSource struct {
	Source Source

	compressedBuf []byte
}

var _ Source = (*DecompressingSource)(nil)

// Fetch first fetches from the underlying source, then decompresses
func (ds *DecompressingSource) Fetch(loc BlockLocation, data []byte) error {
	if ds.compressedBuf == nil {
		// planning for the worst case scenario - that compressing the data
		// actually increased the block size
		ds.compressedBuf = make([]byte, BigBlockSize*2)
	}

	err := ds.Source.Fetch(loc, ds.compressedBuf)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	_, err = zstd.Decompress(data, ds.compressedBuf)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	return nil
}

// GetContainer returns the underlying source's container
func (ds *DecompressingSource) GetContainer() *tlc.Container {
	return ds.Source.GetContainer()
}

// Clone returns a copy of this decomressing source
func (ds *DecompressingSource) Clone() Source {
	return &DecompressingSource{
		Source: ds.Source.Clone(),
	}
}
