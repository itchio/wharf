package bowl_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/tlc"
	"github.com/stretchr/testify/assert"
)

type testFile struct {
	Path string
	Data []byte
}

func findFile(t *testing.T, container *tlc.Container, path string) int64 {
	for i, f := range container.Files {
		if f.Path == path {
			return int64(i)
		}
	}

	must(t, fmt.Errorf("internal error: no such file: %s", path))
	return -1
}

func TestBowl(t *testing.T) {
	// consts
	peacefulContents := []byte("i am not to be disturbed")
	patchedNewContents := []byte("and now i am there")

	targetContainer := &tlc.Container{}
	targetFiles := []testFile{
		testFile{
			Path: "peaceful",
			Data: peacefulContents,
		},
		testFile{
			Path: "patched",
			Data: []byte("i am here"),
		},
		testFile{
			Path: "mover/visitor",
			Data: []byte("i'm going somewhere"),
		},
	}

	for _, tf := range targetFiles {
		targetContainer.Files = append(targetContainer.Files, &tlc.File{
			Path: tf.Path,
			Size: int64(len(tf.Data)),
		})
	}

	sourceContainer := &tlc.Container{}
	sourceContainer.Files = []*tlc.File{
		&tlc.File{
			Path: "peaceful",
			Size: int64(len(peacefulContents)),
			Mode: 0644,
		},
		&tlc.File{
			Path: "patched",
			Size: int64(len(patchedNewContents)),
			Mode: 0644,
		},
		&tlc.File{
			Path: "shaker/visitor",
			Mode: 0644,
		},
		&tlc.File{
			Path: "fresh",
			Mode: 0644,
		},
	}
	sourceContainer.Dirs = []*tlc.Dir{
		&tlc.Dir{
			Path: "shaker",
			Mode: 0755,
		},
	}

	targetDir, err := ioutil.TempDir("", "bowl-target")
	must(t, err)

	defer os.RemoveAll(targetDir)

	for _, tf := range targetFiles {
		fullPath := filepath.Join(targetDir, tf.Path)

		must(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		must(t, ioutil.WriteFile(fullPath, tf.Data, 0644))
	}

	targetPool := fspool.New(targetContainer, targetDir)

	useBowl := func(b bowl.Bowl) {
		var err error

		err = targetPool.Close()
		must(t, err)

		transpose := func(targetPath string, sourcePath string) {
			targetIndex := findFile(t, targetContainer, targetPath)
			sourceIndex := findFile(t, sourceContainer, sourcePath)

			err = b.Transpose(bowl.Transposition{
				TargetIndex: targetIndex,
				SourceIndex: sourceIndex,
			})
		}

		patch := func(sourcePath string, data []byte) {
			sourceIndex := findFile(t, sourceContainer, sourcePath)

			w, err := b.GetWriter(sourceIndex)
			defer func() {
				must(t, w.Close())
			}()

			_, err = w.Write(data)
			must(t, err)
		}

		transpose("peaceful", "peaceful")
		transpose("mover/visitor", "shaker/visitor")

		patch("patched", patchedNewContents)
		patch("fresh", []byte("nobody has seen me before"))

		must(t, targetPool.Close())

		must(t, b.Commit())
	}

	{
		b, err := bowl.NewDryBowl(&bowl.DryBowlParams{
			SourceContainer: sourceContainer,
			TargetContainer: targetContainer,
		})
		must(t, err)

		useBowl(b)
	}

	func() {
		dir, err := ioutil.TempDir("", "fresh-bowl")
		must(t, err)

		defer os.RemoveAll(dir)

		must(t, os.MkdirAll(dir, 0755))

		b, err := bowl.NewFreshBowl(&bowl.FreshBowlParams{
			SourceContainer: sourceContainer,
			TargetContainer: targetContainer,
			TargetPool:      targetPool,

			OutputFolder: dir,
		})
		must(t, err)

		useBowl(b)
	}()

	func() {
		outputDir, err := ioutil.TempDir("", "overlay-bowl")
		must(t, err)
		defer os.RemoveAll(outputDir)
		must(t, os.MkdirAll(outputDir, 0755))

		stageDir, err := ioutil.TempDir("", "overlay-stage")
		must(t, err)
		defer os.RemoveAll(stageDir)
		must(t, os.MkdirAll(stageDir, 0755))

		must(t, ditto(outputDir, targetDir))

		b, err := bowl.NewOverlayBowl(&bowl.OverlayBowlParams{
			SourceContainer: sourceContainer,
			TargetContainer: targetContainer,

			StageFolder:  stageDir,
			OutputFolder: outputDir,
		})
		must(t, err)

		useBowl(b)
	}()
}

func must(t *testing.T, err error) {
	if err != nil {
		if se, ok := err.(*errors.Error); ok {
			t.Logf("Full stack: %s", se.ErrorStack())
		}
		assert.NoError(t, err)
		t.FailNow()
	}
}
