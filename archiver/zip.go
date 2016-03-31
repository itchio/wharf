package archiver

import (
	"archive/zip"
	"io/ioutil"
	"os"
	"path"

	"github.com/itchio/wharf/pwr"
)

func ExtractZip(archive string, dir string, consumer *pwr.StateConsumer) (*ExtractResult, error) {
	consumer.Infof("Extracting %s to %s", archive, dir)

	dirCount := 0
	regCount := 0
	symlinkCount := 0

	reader, err := zip.OpenReader(archive)
	if err != nil {
		return nil, err
	}

	defer reader.Close()

	for _, file := range reader.File {
		err = func() error {
			rel := file.Name
			filename := path.Join(dir, rel)

			info := file.FileInfo()
			mode := info.Mode()

			fileReader, err := file.Open()
			if err != nil {
				return err
			}
			defer fileReader.Close()

			if info.IsDir() {
				err = Mkdir(filename)
				if err != nil {
					return err
				}
				dirCount++
			} else if mode&os.ModeSymlink > 0 {
				linkname, err := ioutil.ReadAll(fileReader)
				err = Symlink(string(linkname), filename, consumer)
				if err != nil {
					return err
				}
				symlinkCount++
			} else {
				err = CopyFile(filename, os.FileMode(mode&LuckyMode|ModeMask), fileReader, consumer)
				if err != nil {
					return err
				}
				regCount++
			}

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return &ExtractResult{
		Dirs:     dirCount,
		Files:    regCount,
		Symlinks: symlinkCount,
	}, nil
}
