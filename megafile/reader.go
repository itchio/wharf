package megafile

import (
	"errors"
	"io"
)

type Reader struct {
	basePath string
	info     *RepoInfo

	offset int64
	file   File
	reader io.ReadSeeker
}

func (info *RepoInfo) NewReader(basePath string) *Reader {
	return &Reader{
		basePath: basePath,
		info:     info,
	}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return 0, errors.New("stub")
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("stub")
}

func (r *Reader) Close() error {
	return nil
}

var _ io.ReadSeeker = (*Reader)(nil)
var _ io.Closer = (*Reader)(nil)
