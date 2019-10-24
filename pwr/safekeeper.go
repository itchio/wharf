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

	blockValidatorLock sync.Mutex
	blockValidator     BlockValidator
	sigError           error

	bufLock sync.Mutex
	buf     []byte

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

		validBlocks: make(map[int64]fileBlocks),

		buf: make([]byte, BlockSize),
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

func (sk *safeKeeper) getBlockValidator() (BlockValidator, error) {
	sk.blockValidatorLock.Lock()
	defer sk.blockValidatorLock.Unlock()

	if sk.blockValidator != nil {
		return sk.blockValidator, nil
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

	hashInfo, err := ComputeHashInfo(sigInfo)
	if err != nil {
		sk.sigError = err
		return nil, err
	}

	sk.blockValidator = NewBlockValidator(hashInfo)
	return sk.blockValidator, nil
}

func (sk *safeKeeper) validateBlock(skr *safeKeeperReader) error {
	sk.bufLock.Lock()
	defer sk.bufLock.Unlock()

	if _, ok := sk.validBlocks[skr.fileIndex]; !ok {
		// first time checking blocks from this file
		sk.validBlocks[skr.fileIndex] = make(fileBlocks)
	}
	blocks := sk.validBlocks[skr.fileIndex]

	blockIndex := skr.offset / BlockSize
	if _, checked := blocks[int(blockIndex)]; !checked {
		bv, err := sk.getBlockValidator()
		if err != nil {
			return err
		}

		blockSize := bv.BlockSize(skr.fileIndex, blockIndex)
		buf := sk.buf[:blockSize]

		savedOffset := skr.offset
		blockOffset := blockIndex * BlockSize
		_, err = skr.rs.Seek(blockOffset, io.SeekStart)
		if err != nil {
			// restore offset
			_, _ = skr.rs.Seek(savedOffset, io.SeekStart)
			return err
		}

		readBytes, err := skr.rs.Read(buf)
		if err != nil {
			// restore offset
			_, _ = skr.rs.Seek(savedOffset, io.SeekStart)
			return err
		}

		data := buf[:readBytes]

		validationError := bv.ValidateAsError(skr.fileIndex, blockIndex, data)
		blocks[int(blockIndex)] = validationError

		// restore offset
		_, _ = skr.rs.Seek(savedOffset, io.SeekStart)
	}
	return blocks[int(blockIndex)]
}

type safeKeeperReader struct {
	// specified
	rs        io.ReadSeeker
	sk        *safeKeeper
	fileIndex int64

	// internal
	offset int64
}

var _ io.ReadSeeker = (*safeKeeperReader)(nil)

func (skr *safeKeeperReader) Seek(off int64, whence int) (int64, error) {
	offset, err := skr.rs.Seek(off, whence)
	skr.offset = offset
	return offset, err
}

func (skr *safeKeeperReader) Read(p []byte) (int, error) {
	err := skr.sk.validateBlock(skr)
	if err != nil {
		return 0, err
	}

	// TODO: check hashes
	n, err := skr.rs.Read(p)
	skr.offset += int64(n)
	return n, err
}
