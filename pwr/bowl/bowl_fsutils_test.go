package bowl_test

import (
	"io"
	"os"
	"path/filepath"

	"github.com/go-errors/errors"
)

func ditto(dst string, src string) error {
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err, 0)
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		dstPath := filepath.Join(dst, relPath)

		switch {
		case info.IsDir():
			err = os.MkdirAll(dstPath, info.Mode())
			if err != nil {
				return errors.Wrap(err, 0)
			}
		case info.Mode()&os.ModeSymlink > 0:
			return errors.New("don't know how to ditto symlink")
		default:
			err = func() error {
				r, err := os.Open(path)
				if err != nil {
					return errors.Wrap(err, 0)
				}
				defer r.Close()

				w, err := os.OpenFile(dstPath, os.O_CREATE, info.Mode())
				if err != nil {
					return errors.Wrap(err, 0)
				}
				defer w.Close()

				_, err = io.Copy(w, r)
				if err != nil {
					return errors.Wrap(err, 0)
				}
				return nil
			}()
			if err != nil {
				return errors.Wrap(err, 0)
			}
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, 0)
	}
	return nil
}
