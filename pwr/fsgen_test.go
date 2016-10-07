package pwr

import (
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

type testDirSettings struct {
	fakeDataSize int64
	seed         int64
}

func makeTestDir(t *testing.T, dir string, s testDirSettings) {
	r := rand.New(rand.NewSource(s.seed))

	assert.Nil(t, os.MkdirAll(dir, 0755))

	assert.Nil(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	fakeData := make([]byte, BlockSize*8)
	r.Read(fakeData)

	createFile := func(name string) {
		f, fErr := os.Create(filepath.Join(dir, name))
		assert.Nil(t, fErr)
		defer f.Close()

		_, fErr = f.Write(fakeData)
		assert.Nil(t, fErr)
	}

	createLink := func(name string, dest string) {
		if !testSymlinks {
			return
		}
		assert.Nil(t, os.Symlink(filepath.Join(dir, dest), filepath.Join(dir, name)))
	}

	for i := 0; i < 4; i++ {
		createFile(fmt.Sprintf("file-%d", i))
	}

	assert.Nil(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	for i := 0; i < 2; i++ {
		createFile(fmt.Sprintf("subdir/file-%d", i))
	}

	createLink("link1", "subdir/file-1")
	createLink("link2", "file-3")
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
