package pwr

import (
	"context"
	"io"
	"sync"

	"github.com/itchio/lake"
	"github.com/itchio/savior"
	"github.com/pkg/errors"
)

type SafeKeeperOpen func() (savior.SeekSource, error)

type fileBlocks = map[int]error

type safeKeeper struct {
	inner lake.Pool

	open SafeKeeperOpen

	sigLock  sync.Mutex
	sigInfo  *SignatureInfo
	sigError error

	validBlocks map[int64]fileBlocks
}

var _ lake.Pool = (*safeKeeper)(nil)

type SafeKeeperParams struct {
	Inner lake.Pool
	Open  SafeKeeperOpen
}

func NewSafeKeeper(params SafeKeeperParams) (lake.Pool, error) {
	if params.Inner == nil {
		return nil, errors.New("SafeKeeperParams.Inner can't be nil")
	}
	if params.Open == nil {
		return nil, errors.New("SafeKeeperParams.Open can't be nil")
	}

	sk := &safeKeeper{
		inner: params.Inner,
		open:  params.Open,
	}
	return sk, nil
}

func (sk *safeKeeper) GetSize(fileIndex int64) int64 {
	return sk.inner.GetSize(fileIndex)
}

func (sk *safeKeeper) GetReadSeeker(fileIndex int64) (io.ReadSeeker, error) {
	rs, err := sk.inner.GetReadSeeker(fileIndex)
	if err != nil {
		return nil, err
	}

	res := &safeKeeperReader{
		sk:        sk,
		rs:        rs,
		fileIndex: fileIndex,
	}
	return res, nil
}

func (sk *safeKeeper) GetReader(fileIndex int64) (io.Reader, error) {
	return sk.GetReadSeeker(fileIndex)
}

func (sk *safeKeeper) Close() error {
	return sk.inner.Close()
}

func (sk *safeKeeper) getSignature() (*SignatureInfo, error) {
	sk.sigLock.Lock()
	defer sk.sigLock.Unlock()

	if sk.sigInfo != nil {
		return sk.sigInfo, nil
	}

	if sk.sigError != nil {
		return nil, sk.sigError
	}

	source, err := sk.open()
	if err != nil {
		// store error so we don't keep retrying to open
		// note that eos has built-in retry logic, so we're fine.
		sk.sigError = err
		return nil, err
	}

	// this seems bad, because it could hang forever, but
	// *hopefully* all our "network file" timeouts are set up
	// correctly so it doesn't.
	ctx := context.Background()

	sigInfo, err := ReadSignature(ctx, source)
	if err != nil {
		sk.sigError = err
		return nil, err
	}

	sk.sigInfo = sigInfo
	return sk.sigInfo, nil
}

type safeKeeperReader struct {
	// specified
	rs        io.ReadSeeker
	sk        *safeKeeper
	fileIndex int64

	// internal
	offset      int64
}

var _ io.ReadSeeker = (*safeKeeperReader)(nil)

func (skr *safeKeeperReader) Seek(off int64, whence int) (int64, error) {
	offset, err := skr.rs.Seek(off, whence)
	skr.offset = offset
	return offset, err
}

func (skr *safeKeeperReader) Read(p []byte) (int, error) {
	blockNumber := skr.offset / BlockSize
	if valid, done := skr.validBlocks[blockblockNumber] {

	}

	sig, err := skr.sk.getSignature()
	if err != nil {
		return 0, err
	}

	// TODO: check hashes
	n, err := skr.rs.Read(p)
	skr.offset += int64(n)
	return n, err
}
