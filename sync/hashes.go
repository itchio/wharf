package sync

import (
	"bufio"
	"bytes"
	"io"
)

func (ctx *SyncContext) splitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) >= ctx.blockSize {
		advance = ctx.blockSize
		token = data[:ctx.blockSize]
		return
	}

	if atEOF {
		advance = len(data)
		token = data
		return
	}

	// wait for more data
	return
}

// Calculate the signature of target.
func (ctx *SyncContext) CreateSignature(FileIndex int64, fileReader io.Reader, writeHash SignatureWriter) error {
	s := bufio.NewScanner(fileReader)
	s.Split(ctx.splitFunc)

	Index := int64(0)

	for s.Scan() {
		block := s.Bytes()
		WeakHash, _, _ := βhash(block)
		StrongHash := ctx.uniqueHash(block)
		blockHash := BlockHash{FileIndex, Index, WeakHash, StrongHash}

		err := writeHash(blockHash)
		if err != nil {
			return err
		}
		Index++
	}

	return nil
}

// Use a more unique way to identify a set of bytes.
func (ctx *SyncContext) uniqueHash(v []byte) []byte {
	ctx.uniqueHasher.Reset()
	ctx.uniqueHasher.Write(v)
	return ctx.uniqueHasher.Sum(nil)
}

// Searches for a given strong hash among all strong hashes in this bucket.
func findUniqueHash(hh []BlockHash, hashValue []byte) *BlockHash {
	if len(hashValue) == 0 {
		return nil
	}
	for _, block := range hh {
		if bytes.Equal(block.StrongHash, hashValue) {
			return &block
		}
	}
	return nil
}

// Use a faster way to identify a set of bytes.
func βhash(block []byte) (β uint32, β1 uint32, β2 uint32) {
	var a, b uint32
	for i, val := range block {
		a += uint32(val)
		b += (uint32(len(block)-1) - uint32(i) + 1) * uint32(val)
	}
	β = (a % _M) + (_M * (b % _M))
	β1 = a % _M
	β2 = b % _M
	return
}
