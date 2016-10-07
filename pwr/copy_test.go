package pwr

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
)

func Test_CopyContainer(t *testing.T) {
	mainDir, err := ioutil.TempDir("", "copycontainer")
	assert.Nil(t, err)
	defer os.RemoveAll(mainDir)

	src := path.Join(mainDir, "src")
	dst := path.Join(mainDir, "dst")
	makeTestDir(t, src, testDirSettings{
		fakeDataSize: 4,
		seed:         0x91738,
	})

	container, err := tlc.WalkAny(src, nil)
	assert.Nil(t, err)

	inPool := fspool.New(container, src)
	outPool := fspool.New(container, dst)

	err = CopyContainer(container, outPool, inPool, &state.Consumer{})
	assert.Nil(t, err)
}
