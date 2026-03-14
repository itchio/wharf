package archiver

import (
	"archive/tar"
	"bytes"
	stdErrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/itchio/arkive/zip"
	"github.com/itchio/headway/state"
	"github.com/stretchr/testify/assert"
)

var testSymlinks bool = (runtime.GOOS != "windows")

func makeTestDir(t *testing.T, dir string) {
	assert.NoError(t, os.MkdirAll(dir, 0755))

	assert.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	createFile := func(name string) {
		f, fErr := os.Create(filepath.Join(dir, name))
		assert.NoError(t, fErr)
		defer f.Close()

		_, fErr = f.Write([]byte{4, 3, 2, 1})
		assert.NoError(t, fErr)
	}

	createLink := func(name string, dest string) {
		if !testSymlinks {
			return
		}
		assert.NoError(t, os.Symlink(dest, filepath.Join(dir, name)))
	}

	for i := range 4 {
		createFile(fmt.Sprintf("file-%d", i))
	}

	assert.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	for i := range 2 {
		createFile(fmt.Sprintf("subdir/file-%d", i))
	}

	createLink("link1", "subdir/file-1")
	createLink("link2", "file-3")
}

func mustWriteFile(t *testing.T, path string, data string) {
	t.Helper()

	assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	assert.NoError(t, os.WriteFile(path, []byte(data), 0644))
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	assert.NoError(t, err)
	return string(data)
}

type zipTestEntry struct {
	name string
	body string
	mode os.FileMode
}

func buildZip(t *testing.T, entries []zipTestEntry) []byte {
	t.Helper()

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name}
		header.SetMode(entry.mode)

		w, err := zw.CreateHeader(header)
		assert.NoError(t, err)

		_, err = io.WriteString(w, entry.body)
		assert.NoError(t, err)
	}

	assert.NoError(t, zw.Close())
	return buf.Bytes()
}

type tarTestEntry struct {
	name     string
	body     string
	typeflag byte
	linkname string
	mode     int64
}

func buildTar(t *testing.T, entries []tarTestEntry) string {
	t.Helper()

	f, err := os.CreateTemp("", "wharf-archiver-*.tar")
	assert.NoError(t, err)
	defer f.Close()

	tw := tar.NewWriter(f)
	for _, entry := range entries {
		header := &tar.Header{
			Name:     entry.name,
			Typeflag: entry.typeflag,
			Linkname: entry.linkname,
			Mode:     entry.mode,
			Size:     int64(len(entry.body)),
		}

		if entry.typeflag == tar.TypeSymlink {
			header.Size = 0
		}

		assert.NoError(t, tw.WriteHeader(header))
		if entry.typeflag == tar.TypeReg {
			_, err := io.WriteString(tw, entry.body)
			assert.NoError(t, err)
		}
	}

	assert.NoError(t, tw.Close())
	return f.Name()
}

func Test_ZipUnzip(t *testing.T) {
	tmpPath, err := os.MkdirTemp("", "zipunzip")
	assert.NoError(t, err)

	defer os.RemoveAll(tmpPath)
	assert.NoError(t, os.MkdirAll(tmpPath, 0755))

	dir := filepath.Join(tmpPath, "dir")
	makeTestDir(t, dir)

	extractedDir := filepath.Join(tmpPath, "extractedDir")
	archivePath := filepath.Join(tmpPath, "archive.zip")

	archiveWriter, err := os.Create(archivePath)
	assert.NoError(t, err)
	defer archiveWriter.Close()

	_, err = CompressZip(archiveWriter, dir, &state.Consumer{})
	assert.NoError(t, err)

	xSettings := ExtractSettings{
		Consumer: &state.Consumer{},
	}

	t.Logf("Extracting over non-existent destination")
	_, err = ExtractPath(archivePath, extractedDir, xSettings)
	assert.NoError(t, err)

	resumeFilePath := filepath.Join(tmpPath, "resumeinfo")

	t.Logf("Extracting over already-extracted dir")
	_, err = ExtractPath(archivePath, extractedDir, ExtractSettings{
		Consumer:   xSettings.Consumer,
		ResumeFrom: resumeFilePath,
	})
	assert.NoError(t, err)

	t.Logf("Extracting over already-extracted dir (with resume)")
	_, err = ExtractPath(archivePath, extractedDir, ExtractSettings{
		Consumer:   xSettings.Consumer,
		ResumeFrom: resumeFilePath,
	})
	assert.NoError(t, err)

	t.Logf("Extracting, one of the dirs is a file now")
	assert.NoError(t, os.RemoveAll(filepath.Join(extractedDir, "subdir")))
	dumbFile, err := os.Create(filepath.Join(extractedDir, "subdir"))
	assert.NoError(t, err)
	assert.NoError(t, dumbFile.Close())

	_, err = ExtractPath(archivePath, extractedDir, xSettings)
	assert.NoError(t, err)
}

func Test_TarUntar(t *testing.T) {
	tmpPath, err := os.MkdirTemp("", "taruntar")
	assert.NoError(t, err)

	defer os.RemoveAll(tmpPath)
	assert.NoError(t, os.MkdirAll(tmpPath, 0755))

	dir := filepath.Join(tmpPath, "dir")
	makeTestDir(t, dir)

	extractedDir := filepath.Join(tmpPath, "extractedDir")
	archivePath := filepath.Join(tmpPath, "archive.tar")

	archiveWriter, err := os.Create(archivePath)
	assert.NoError(t, err)
	defer archiveWriter.Close()

	_, err = CompressTar(archiveWriter, dir, &state.Consumer{})
	assert.NoError(t, err)

	xSettings := ExtractSettings{
		Consumer: &state.Consumer{},
	}

	t.Logf("Extracting over non-existent destination")
	_, err = ExtractTar(archivePath, extractedDir, xSettings)
	assert.NoError(t, err)

	t.Logf("Extracting over already-extracted dir")
	_, err = ExtractTar(archivePath, extractedDir, xSettings)
	assert.NoError(t, err)

	t.Logf("Extracting, one of the dirs is a file now")
	assert.NoError(t, os.RemoveAll(filepath.Join(extractedDir, "subdir")))
	dumbFile, err := os.Create(filepath.Join(extractedDir, "subdir"))
	assert.NoError(t, err)
	assert.NoError(t, dumbFile.Close())

	_, err = ExtractTar(archivePath, extractedDir, xSettings)
	assert.NoError(t, err)
}

func Test_ZipRejectsPathTraversal(t *testing.T) {
	tmpPath, err := os.MkdirTemp("", "zip-path-traversal")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	destDir := filepath.Join(tmpPath, "dest")
	outsidePath := filepath.Join(tmpPath, "outside.txt")
	mustWriteFile(t, outsidePath, "before")

	payload := buildZip(t, []zipTestEntry{
		{name: "../outside.txt", body: "zip-owned", mode: 0644},
	})

	_, err = Extract(bytes.NewReader(payload), int64(len(payload)), destDir, ExtractSettings{
		Consumer:    &state.Consumer{},
		Concurrency: 1,
	})
	assert.Error(t, err)
	assert.True(t, stdErrors.Is(err, ErrPathTraversal))
	assert.Equal(t, "before", readFile(t, outsidePath))
}

func Test_TarRejectsPathTraversal(t *testing.T) {
	tmpPath, err := os.MkdirTemp("", "tar-path-traversal")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	destDir := filepath.Join(tmpPath, "dest")
	outsidePath := filepath.Join(tmpPath, "outside.txt")
	mustWriteFile(t, outsidePath, "before")

	archivePath := buildTar(t, []tarTestEntry{
		{name: "../outside.txt", body: "tar-owned", typeflag: tar.TypeReg, mode: 0644},
	})
	defer os.Remove(archivePath)

	_, err = ExtractTar(archivePath, destDir, ExtractSettings{Consumer: &state.Consumer{}})
	assert.Error(t, err)
	assert.True(t, stdErrors.Is(err, ErrPathTraversal))
	assert.Equal(t, "before", readFile(t, outsidePath))
}

func Test_TarRejectsExternalReplacement(t *testing.T) {
	tmpPath, err := os.MkdirTemp("", "tar-external-replacement")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	destDir := filepath.Join(tmpPath, "dest")
	victimDir := filepath.Join(tmpPath, "victim")
	nestedPath := filepath.Join(victimDir, "nested.txt")
	mustWriteFile(t, nestedPath, "keep-me")

	archivePath := buildTar(t, []tarTestEntry{
		{name: "../victim", body: "replacement", typeflag: tar.TypeReg, mode: 0644},
	})
	defer os.Remove(archivePath)

	_, err = ExtractTar(archivePath, destDir, ExtractSettings{Consumer: &state.Consumer{}})
	assert.Error(t, err)
	assert.True(t, stdErrors.Is(err, ErrPathTraversal))

	info, statErr := os.Stat(victimDir)
	assert.NoError(t, statErr)
	assert.True(t, info.IsDir())
	assert.Equal(t, "keep-me", readFile(t, nestedPath))
}

func Test_TarRejectsSymlinkTraversal(t *testing.T) {
	if !testSymlinks {
		t.Skip("symlink tests not applicable on Windows")
	}

	tmpPath, err := os.MkdirTemp("", "tar-symlink-traversal")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	destDir := filepath.Join(tmpPath, "dest")
	outsidePath := filepath.Join(tmpPath, "pivoted.txt")
	mustWriteFile(t, outsidePath, "before")

	archivePath := buildTar(t, []tarTestEntry{
		{name: "pivot", typeflag: tar.TypeSymlink, linkname: "..", mode: 0777},
		{name: "pivot/pivoted.txt", body: "tar-owned", typeflag: tar.TypeReg, mode: 0644},
	})
	defer os.Remove(archivePath)

	_, err = ExtractTar(archivePath, destDir, ExtractSettings{Consumer: &state.Consumer{}})
	assert.Error(t, err)
	assert.True(t, stdErrors.Is(err, ErrPathTraversal))
	assert.Equal(t, "before", readFile(t, outsidePath))
}

func Test_ZipRejectsSymlinkTraversal(t *testing.T) {
	if !testSymlinks {
		t.Skip("symlink tests not applicable on Windows")
	}

	tmpPath, err := os.MkdirTemp("", "zip-symlink-traversal")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpPath)

	destDir := filepath.Join(tmpPath, "dest")
	outsidePath := filepath.Join(tmpPath, "pivoted.txt")
	mustWriteFile(t, outsidePath, "before")

	payload := buildZip(t, []zipTestEntry{
		{name: "pivot", body: "..", mode: os.ModeSymlink | 0777},
		{name: "pivot/pivoted.txt", body: "zip-owned", mode: 0644},
	})

	_, err = Extract(bytes.NewReader(payload), int64(len(payload)), destDir, ExtractSettings{
		Consumer:    &state.Consumer{},
		Concurrency: 1,
	})
	assert.Error(t, err)
	assert.True(t, stdErrors.Is(err, ErrPathTraversal))
	assert.Equal(t, "before", readFile(t, outsidePath))
}
