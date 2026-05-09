package archiver

import (
	stdErrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/itchio/headway/state"
	"github.com/itchio/httpkit/eos"
	"github.com/itchio/httpkit/eos/option"
	"github.com/itchio/httpkit/htfs"

	"github.com/pkg/errors"
)

const (
	// ModeMask is or'd with files walked by butler
	ModeMask = 0666

	// LuckyMode is used when wiping in last-chance mode
	LuckyMode = 0777

	// DirMode is the default mode for directories created by butler
	DirMode = 0755
)

type ExtractResult struct {
	Dirs     int
	Files    int
	Symlinks int
}

type CompressResult struct {
	UncompressedSize int64
	CompressedSize   int64
}

type UncompressedSizeKnownFunc func(uncompressedSize int64)

type EntryDoneFunc func(slashPath string)

// ErrPathTraversal is returned when an archive entry attempts to escape the
// extraction directory or write through a symlinked parent path.
var ErrPathTraversal = stdErrors.New("path traversal detected")

type ExtractSettings struct {
	Consumer                *state.Consumer
	ResumeFrom              string
	OnUncompressedSizeKnown UncompressedSizeKnownFunc
	OnEntryDone             EntryDoneFunc
	DryRun                  bool
	Concurrency             int
}

func ExtractPath(archive string, destPath string, settings ExtractSettings) (*ExtractResult, error) {
	var result *ExtractResult
	var err error

	file, err := eos.Open(archive, option.WithConsumer(settings.Consumer))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if _, ok := file.(*htfs.File); ok {
		settings.Consumer.Infof("Extracting remote file, forcing concurrency to 1")
		settings.Concurrency = 1
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		if cErr := file.Close(); cErr != nil && err == nil {
			err = errors.WithStack(cErr)
		}
	}()

	result, err = Extract(file, stat.Size(), destPath, settings)

	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func Extract(readerAt io.ReaderAt, size int64, destPath string, settings ExtractSettings) (*ExtractResult, error) {
	result, err := ExtractZip(readerAt, size, destPath, settings)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func resolveDestPath(baseDir string, name string) (string, error) {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", errors.WithStack(err)
	}

	joined := filepath.Join(absBase, filepath.FromSlash(name))
	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", errors.WithStack(err)
	}

	if absJoined != absBase && !strings.HasPrefix(absJoined, absBase+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", ErrPathTraversal, name)
	}

	return absJoined, nil
}

func ensureSafeParents(baseDir string, dstpath string) error {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return errors.WithStack(err)
	}

	relPath, err := filepath.Rel(absBase, dstpath)
	if err != nil {
		return errors.WithStack(err)
	}

	parentRel := filepath.Dir(relPath)
	if parentRel == "." {
		return nil
	}

	current := absBase
	for part := range strings.SplitSeq(parentRel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}

		current = filepath.Join(current, part)

		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return errors.WithStack(err)
		}

		if info.Mode()&os.ModeSymlink > 0 {
			return fmt.Errorf("%w: parent path contains symlink: %s", ErrPathTraversal, current)
		}

		if !info.IsDir() {
			return errors.Errorf("parent path is not a directory: %s", current)
		}
	}

	return nil
}

func resolveExtractPath(baseDir string, name string) (string, error) {
	dstpath, err := resolveDestPath(baseDir, name)
	if err != nil {
		return "", err
	}

	err = ensureSafeParents(baseDir, dstpath)
	if err != nil {
		return "", err
	}

	return dstpath, nil
}

func validateSymlinkTarget(baseDir string, dstpath string, linkname string) error {
	if filepath.IsAbs(linkname) {
		return fmt.Errorf("%w: absolute symlink target: %s", ErrPathTraversal, linkname)
	}

	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return errors.WithStack(err)
	}

	resolvedTarget := filepath.Join(filepath.Dir(dstpath), linkname)
	absTarget, err := filepath.Abs(resolvedTarget)
	if err != nil {
		return errors.WithStack(err)
	}

	if absTarget != absBase && !strings.HasPrefix(absTarget, absBase+string(filepath.Separator)) {
		return fmt.Errorf("%w: symlink target escapes destination: %s", ErrPathTraversal, linkname)
	}

	return nil
}

func Mkdir(dstpath string) error {
	dirstat, err := os.Lstat(dstpath)
	if err != nil {
		// main case - dir doesn't exist yet
		err = os.MkdirAll(dstpath, DirMode)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	if dirstat.IsDir() {
		// is already a dir, good!
	} else {
		// is a file or symlink for example, turn into a dir
		err = os.Remove(dstpath)
		if err != nil {
			return errors.WithStack(err)
		}
		err = os.MkdirAll(dstpath, DirMode)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func Symlink(linkname string, filename string, consumer *state.Consumer) error {
	consumer.Debugf("ln -s %s %s", linkname, filename)

	err := os.RemoveAll(filename)
	if err != nil {
		return errors.WithStack(err)
	}

	dirname := filepath.Dir(filename)
	err = os.MkdirAll(dirname, LuckyMode)
	if err != nil {
		return errors.WithStack(err)
	}

	err = os.Symlink(linkname, filename)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func CopyFile(filename string, mode os.FileMode, fileReader io.Reader) error {
	err := os.RemoveAll(filename)
	if err != nil {
		return errors.WithStack(err)
	}

	dirname := filepath.Dir(filename)
	err = os.MkdirAll(dirname, LuckyMode)
	if err != nil {
		return errors.WithStack(err)
	}

	writer, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return errors.WithStack(err)
	}
	defer writer.Close()

	_, err = io.Copy(writer, fileReader)
	if err != nil {
		return errors.WithStack(err)
	}

	err = os.Chmod(filename, mode)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
