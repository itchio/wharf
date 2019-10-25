package pwr

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/wharf/wtest"
	"github.com/stretchr/testify/assert"
)

func Test_CopyContainer(t *testing.T) {
	mainDir, err := ioutil.TempDir("", "copycontainer")
	assert.NoError(t, err)
	defer os.RemoveAll(mainDir)

	src := path.Join(mainDir, "src")
	dst := path.Join(mainDir, "dst")
	wtest.MakeTestDir(t, src, wtest.TestDirSettings{
		Seed: 0x91738,
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1},
			{Path: "file-1", Seed: 0x2},
			{Path: "file-2", Seed: 0x3},
		},
	})

	container, err := tlc.WalkAny(src, &tlc.WalkOpts{})
	assert.NoError(t, err)

	inPool := fspool.New(container, src)
	outPool := fspool.New(container, dst)

	err = CopyContainer(container, outPool, inPool, &state.Consumer{})
	assert.NoError(t, err)

	assert.NoError(t, inPool.Close())
}
