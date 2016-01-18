package counter_test

import (
	"io/ioutil"
	"testing"

	"github.com/itchio/wharf.proto/counter"
	"github.com/stretchr/testify/assert"
)

func Test_Count(t *testing.T) {
	cw := counter.New(ioutil.Discard)
	buf := []byte{1, 2, 3, 4, 5, 6}
	for i := 0; i < 6; i++ {
		cw.Write(buf)
	}

	assert.Equal(t, cw.Count(), int64(36))
}

func Test_NilWriter(t *testing.T) {
	cw := counter.New(nil)
	buf := []byte{1, 2, 3, 4, 5, 6}
	for i := 0; i < 6; i++ {
		cw.Write(buf)
	}

	assert.Equal(t, cw.Count(), int64(36))
}

func Test_Callback(t *testing.T) {
	count := int64(-1)
	onWrite := func(c int64) { count = c }

	cw := counter.NewWithCallback(onWrite, nil)
	buf := []byte{1, 2, 3, 4, 5, 6}

	cw.Write(buf)
	assert.Equal(t, count, int64(6))

	cw.Write(buf)
	assert.Equal(t, count, int64(12))

	cw.Write(buf)
	assert.Equal(t, count, int64(18))

	cw.Write(buf)
	assert.Equal(t, count, int64(24))
}
