package drip

import (
	"fmt"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/itchio/wharf/counter"
)

func Test_Writer(t *testing.T) {
	dropSize := 16

	numShort := 0
	shortSize := 0
	validate := func(buf []byte) error {
		switch {
		case len(buf) == dropSize:
			if numShort > 0 {
				return fmt.Errorf("got full after short")
			}
			return nil
		case len(buf) < dropSize:
			if numShort > 0 {
				return fmt.Errorf("got second short (%d)", len(buf))
			}
			numShort++
			shortSize = len(buf)
		default:
			return fmt.Errorf("drop too large (%d > %d)", len(buf), dropSize)
		}

		return nil
	}

	buf := make([]byte, dropSize)
	countingWriter := counter.NewWriter(nil)

	dw := &Writer{
		Buffer:   buf,
		Validate: validate,
		Writer:   countingWriter,
	}

	rbuf := make([]byte, 128)

	write := func(l int) {
		written, wErr := dw.Write(rbuf[0:l])
		assert.Equal(t, l, written)
		assert.Nil(t, wErr)
	}

	write(12)
	write(4)
	write(10)
	write(6)
	write(16)
	write(64)
	write(5)

	assert.Nil(t, dw.Close())
	assert.Equal(t, 5, shortSize)
	assert.Equal(t, int64(12+4+10+6+16+64+5), countingWriter.Count())
}
