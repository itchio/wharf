package patcher_test

import (
	"io"
	"runtime"
	"testing"

	"github.com/itchio/lake/tlc"
	"github.com/itchio/wharf/wtest"
)

type patchScenario struct {
	name         string
	v1           wtest.TestDirSettings
	intermediate *wtest.TestDirSettings
	corruptions  *testCorruption
	v2           wtest.TestDirSettings
}

type testCorruption struct {
	before func(t *testing.T, dir string)
	files  wtest.TestDirSettings
	after  func(t *testing.T, dir string)
}

const largeAmount int64 = 16

var testSymlinks = (runtime.GOOS != "windows")

type nopCloserWriter struct {
	writer io.Writer
}

var _ io.Writer = (*nopCloserWriter)(nil)

func (ncw *nopCloserWriter) Write(buf []byte) (int, error) {
	return ncw.writer.Write(buf)
}

func applyCorruptions(t *testing.T, dir string, c testCorruption) {
	dump := func() {
		container, err := tlc.WalkAny(dir, &tlc.WalkOpts{})
		wtest.Must(t, err)
		container.Print(func(line string) {
			t.Logf("%s", line)
		})
	}

	t.Logf("=================================")
	t.Logf("Before corruptions:")
	dump()

	if c.before != nil {
		c.before(t, dir)
	}
	wtest.MakeTestDir(t, dir, c.files)
	if c.after != nil {
		c.after(t, dir)
	}

	t.Logf("---------------------------------")
	t.Logf("After corruptions:")
	dump()
	t.Logf("=================================")
}
