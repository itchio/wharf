package pwr

import (
	"os"
	"testing"

	"github.com/itchio/wharf/wtest"
)

func Test_SafeKeeper(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	wtest.Must(t, err)

	wtest.MakeTestDir(t, dir, wtest.TestDirSettings{})
}
