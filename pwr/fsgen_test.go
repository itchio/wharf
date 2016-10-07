package pwr

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/alecthomas/assert"
)

var testSymlinks bool = (runtime.GOOS != "windows")

type testDirEntry struct {
	path string
	mode int
	size int64
	seed int64
}

type testDirSettings struct {
	seed    int64
	entries []testDirEntry
}

func makeTestDir(t *testing.T, dir string, s testDirSettings) {
	prng := rand.New(rand.NewSource(s.seed))

	assert.Nil(t, os.MkdirAll(dir, 0755))
	data := new(bytes.Buffer)

	for _, entry := range s.entries {
		path := filepath.Join(dir, filepath.FromSlash(entry.path))

		parent := filepath.Dir(path)
		mkErr := os.MkdirAll(parent, 0755)
		if mkErr != nil {
			if !os.IsExist(mkErr) {
				assert.Nil(t, mkErr)
			}
		}

		if entry.seed == 0 {
			prng.Seed(s.seed)
		} else {
			prng.Seed(entry.seed)
		}

		data.Reset()
		data.Grow(int(entry.size))

		func() {
			mode := 0644
			if entry.mode != 0 {
				mode = entry.mode
			}

			size := BlockSize*8 + 64
			if entry.size != 0 {
				size = entry.size
			}

			f, fErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.FileMode(mode))
			assert.Nil(t, fErr)
			defer f.Close()

			_, fErr = io.CopyN(f, prng, size)
			assert.Nil(t, fErr)
		}()
	}
}

func cpFile(t *testing.T, src string, dst string) {
	sf, fErr := os.Open(src)
	assert.Nil(t, fErr)
	defer sf.Close()

	info, fErr := sf.Stat()
	assert.Nil(t, fErr)

	df, fErr := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, info.Mode())
	assert.Nil(t, fErr)
	defer df.Close()

	_, fErr = io.Copy(df, sf)
	assert.Nil(t, fErr)
}

func cpDir(t *testing.T, src string, dst string) {
	assert.Nil(t, os.MkdirAll(dst, 0755))

	assert.Nil(t, filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		assert.Nil(t, err)
		name, fErr := filepath.Rel(src, path)
		assert.Nil(t, fErr)

		dstPath := filepath.Join(dst, name)

		if info.IsDir() {
			assert.Nil(t, os.MkdirAll(dstPath, info.Mode()))
		} else if info.Mode()&os.ModeSymlink > 0 {
			dest, fErr := os.Readlink(path)
			assert.Nil(t, fErr)

			assert.Nil(t, os.Symlink(dest, dstPath))
		} else if info.Mode().IsRegular() {
			df, fErr := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY, info.Mode())
			assert.Nil(t, fErr)
			defer df.Close()

			sf, fErr := os.Open(path)
			assert.Nil(t, fErr)
			defer sf.Close()

			_, fErr = io.Copy(df, sf)
			assert.Nil(t, fErr)
		} else {
			return fmt.Errorf("not regular, not symlink, not dir, what is it? %s", path)
		}

		return nil
	}))
}

func assertDirEmpty(t *testing.T, dir string) {
	files, err := ioutil.ReadDir(dir)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(files))
}
