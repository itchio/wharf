package blockpool_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/alecthomas/assert"
	"github.com/itchio/wharf/pools/blockpool"
	"github.com/itchio/wharf/tlc"

	"net/http"
	_ "net/http/pprof"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

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

type TestSink struct {
	FailingBlock blockpool.BlockLocation
}

var _ blockpool.Sink = (*TestSink)(nil)

func (ts *TestSink) Clone() blockpool.Sink {
	return ts
}

func (ts *TestSink) Store(location blockpool.BlockLocation, data []byte) error {
	time.Sleep(10 * time.Millisecond)
	if location.FileIndex == ts.FailingBlock.FileIndex && location.BlockIndex == ts.FailingBlock.BlockIndex {
		return fmt.Errorf("sample fail!")
	}

	return nil
}

func (ts *TestSink) GetContainer() *tlc.Container {
	return nil
}

func Test_FanOut(t *testing.T) {
	t.Logf("Testing fail fast...")

	ts := &TestSink{
		FailingBlock: blockpool.BlockLocation{
			FileIndex:  2,
			BlockIndex: 2,
		},
	}
	fos, err := blockpool.NewFanOutSink(ts, 8)
	assert.Nil(t, err)

	fos.Start()

	hadError := false

	for i := 0; i < 8; i++ {
		for j := 0; j < 8; j++ {
			loc := blockpool.BlockLocation{
				FileIndex:  int64(i),
				BlockIndex: int64(j),
			}
			err := fos.Store(loc, []byte{})
			if err != nil {
				hadError = true
			}
		}
	}

	assert.True(t, hadError)

	err = fos.Close()
	assert.Nil(t, err)

	t.Logf("Testing tail errors...")

	fos, err = blockpool.NewFanOutSink(ts, 8)
	assert.Nil(t, err)

	fos.Start()

	// Store shouldn't err, just queue it...
	err = fos.Store(ts.FailingBlock, []byte{})
	assert.Nil(t, err)

	// but close should catch the error
	err = fos.Close()
	assert.NotNil(t, err)
}
