package tlc_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/itchio/wharf/tlc"
	"github.com/stretchr/testify/assert"
)

func Test_Read(t *testing.T) {
	tmpPath := mktestdir(t, "read")
	defer os.RemoveAll(tmpPath)

	t.Logf("walking sample dir")
	container, err := tlc.Walk(tmpPath, 16)
	must(t, err)

	r := container.NewReader(tmpPath).Boundless()

	t.Logf("reading whole tlc")
	all, err := ioutil.ReadAll(r)
	must(t, err)

	t.Logf("testing tlc layout")
	assert.Equal(t, int64(len(all)), container.Size, "has right length")

	expected := make([]byte, 0, 0)
	expected = appendFiller(expected, 0xa, 10)
	expected = appendFiller(expected, 0xb, 20)
	expected = appendFiller(expected, 0xc, 30)
	expected = appendFiller(expected, 0xd, 50)
	expected = appendFiller(expected, 0xe, 40)

	assert.Equal(t, all, expected, "has padded file layout")

	t.Logf("testing tlc random access")
	single := make([]byte, 1)

	offset, err := r.Seek(0, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(0), "seeks to beginning")

	read, err := r.Read(single)
	must(t, err)
	assert.Equal(t, read, 1, "read 1 byte")
	assert.Equal(t, single[0], byte(0xa), "reads from first file")

	offset, err = r.Seek(11, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(11), "seeks inside second file")

	read, err = r.Read(single)
	must(t, err)
	assert.Equal(t, read, 1, "read 1 byte")
	assert.Equal(t, single[0], byte(0xb), "reads from second file")

	// reading from multiple files

	multi := make([]byte, 30)
	expectedMulti := make([]byte, 0)
	expectedMulti = appendFiller(expectedMulti, 0xa, 5)
	expectedMulti = appendFiller(expectedMulti, 0xb, 20)
	expectedMulti = appendFiller(expectedMulti, 0xc, 5)

	offset, err = r.Seek(5, os.SEEK_SET)
	must(t, err)
	assert.Equal(t, offset, int64(5), "seeks inside first file")

	read, err = r.Read(multi)
	must(t, err)
	assert.Equal(t, read, 30, "read all bytes")
	assert.Equal(t, multi[:read], expectedMulti, "reads across two files")
}

func appendFiller(slice []byte, b byte, q int) []byte {
	for i := 0; i < q; i++ {
		slice = append(slice, b)
	}
	return slice
}
