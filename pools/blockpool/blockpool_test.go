package blockpool_test

import (
	"testing"

	"github.com/alecthomas/assert"
	"github.com/itchio/wharf/pools/blockpool"
)

func Test_BlockMath(t *testing.T) {
	// number of blocks
	assert.Equal(t, int64(0), blockpool.ComputeNumBlocks(0))
	assert.Equal(t, int64(1), blockpool.ComputeNumBlocks(1))
	assert.Equal(t, int64(1), blockpool.ComputeNumBlocks(blockpool.BigBlockSize-1))
	assert.Equal(t, int64(1), blockpool.ComputeNumBlocks(blockpool.BigBlockSize))
	assert.Equal(t, int64(2), blockpool.ComputeNumBlocks(blockpool.BigBlockSize+1))
	assert.Equal(t, int64(2), blockpool.ComputeNumBlocks(blockpool.BigBlockSize*2-1))
	assert.Equal(t, int64(3), blockpool.ComputeNumBlocks(blockpool.BigBlockSize*2+1))

	// block sizes
	assert.Equal(t, blockpool.BigBlockSize-1, blockpool.ComputeBlockSize(blockpool.BigBlockSize-1, 0))

	assert.Equal(t, blockpool.BigBlockSize, blockpool.ComputeBlockSize(blockpool.BigBlockSize, 0))

	assert.Equal(t, blockpool.BigBlockSize, blockpool.ComputeBlockSize(blockpool.BigBlockSize+1, 0))
	assert.Equal(t, int64(1), blockpool.ComputeBlockSize(blockpool.BigBlockSize+1, 1))

	assert.Equal(t, blockpool.BigBlockSize, blockpool.ComputeBlockSize(blockpool.BigBlockSize*2-1, 0))
	assert.Equal(t, blockpool.BigBlockSize-1, blockpool.ComputeBlockSize(blockpool.BigBlockSize*2-1, 1))

	assert.Equal(t, blockpool.BigBlockSize, blockpool.ComputeBlockSize(blockpool.BigBlockSize*2+1, 0))
	assert.Equal(t, blockpool.BigBlockSize, blockpool.ComputeBlockSize(blockpool.BigBlockSize*2+1, 1))
	assert.Equal(t, int64(1), blockpool.ComputeBlockSize(blockpool.BigBlockSize*2+1, 2))
}
