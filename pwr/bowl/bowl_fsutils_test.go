package bowl_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wsync"
	"github.com/stretchr/testify/assert"
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

//////////////////////////////
// bowler
//////////////////////////////

type bowler struct {
	t *testing.T

	TargetContainer *tlc.Container
	SourceContainer *tlc.Container
	TargetPool      wsync.Pool

	TargetFolder string
	RefFolder    string
	FreshFolder  string
}

type bowlMode int

const (
	bowlModeNoop    bowlMode = 0
	bowlModeFresh   bowlMode = 1
	bowlModeInPlace bowlMode = 2
)

type bowlerParams struct {
	makeTarget func(p *bowlerPreparator)
	makeBowl   func(p *makeBowlParams) (bowl.Bowl, bowlMode)
	apply      func(s *bowlerSimulator)
}

type makeBowlParams struct {
	TargetContainer *tlc.Container
	SourceContainer *tlc.Container
	TargetPool      wsync.Pool
	TargetFolder    string
	FreshFolder     string
}

func runBowler(t *testing.T, params *bowlerParams) {
	b := &bowler{
		t: t,
	}

	targetFolder, err := ioutil.TempDir("", "bowler-target")
	must(t, err)
	defer os.RemoveAll(targetFolder)
	b.TargetFolder = targetFolder

	refFolder, err := ioutil.TempDir("", "bowler-reference")
	must(t, err)
	defer os.RemoveAll(refFolder)
	b.RefFolder = refFolder

	freshFolder, err := ioutil.TempDir("", "bowler-fresh")
	must(t, err)
	defer os.RemoveAll(freshFolder)
	b.FreshFolder = freshFolder

	// fill up our target folder + target container
	b.TargetContainer = &tlc.Container{}
	bp := &bowlerPreparator{
		b: b,
	}
	params.makeTarget(bp)

	targetContainer, err := tlc.WalkDir(targetFolder, &tlc.WalkOpts{
		Filter: tlc.DefaultFilter,
	})
	must(t, err)
	b.TargetContainer = targetContainer

	targetPool := fspool.New(b.TargetContainer, targetFolder)
	b.TargetPool = targetPool

	// fill up our ref folder + source container
	bs := &bowlerSimulator{
		b:    b,
		mode: bowlerSimulatorModeMakeReference,
	}
	params.apply(bs)

	sourceContainer, err := tlc.WalkDir(refFolder, &tlc.WalkOpts{
		Filter: tlc.DefaultFilter,
	})
	must(t, err)
	b.SourceContainer = sourceContainer

	// now create the bowl
	bowl, bowlmode := params.makeBowl(&makeBowlParams{
		TargetContainer: b.TargetContainer,
		SourceContainer: b.SourceContainer,
		TargetPool:      targetPool,
		TargetFolder:    targetFolder,
		FreshFolder:     freshFolder,
	})

	// now apply for real
	bs = &bowlerSimulator{
		b:    b,
		mode: bowlerSimulatorModeApply,
		bowl: bowl,
	}
	params.apply(bs)

	// and commit
	must(t, targetPool.Close())
	must(t, bowl.Commit())

	// and now check!
	var outFolder = ""
	switch bowlmode {
	case bowlModeFresh:
		outFolder = freshFolder
	case bowlModeInPlace:
		outFolder = targetFolder
	}

	if outFolder != "" {
		refContainer, err := tlc.WalkDir(refFolder, &tlc.WalkOpts{Filter: tlc.DefaultFilter})
		must(t, err)
		outContainer, err := tlc.WalkDir(outFolder, &tlc.WalkOpts{Filter: tlc.DefaultFilter})
		must(t, err)

		must(t, refContainer.EnsureEqual(outContainer))
		must(t, outContainer.EnsureEqual(refContainer))

		refPool := fspool.New(refContainer, refFolder)
		outPool := fspool.New(refContainer, outFolder)

		for index := range refContainer.Files {
			refReader, err := refPool.GetReader(int64(index))
			must(t, err)
			refBytes, err := ioutil.ReadAll(refReader)
			must(t, err)

			outReader, err := outPool.GetReader(int64(index))
			must(t, err)
			outBytes, err := ioutil.ReadAll(outReader)
			must(t, err)

			assert.EqualValues(t, refBytes, outBytes)
		}
	}
}

// preparator

type bowlerPreparator struct {
	b *bowler
}

func (bp *bowlerPreparator) dir(path string) {
	t := bp.b.t

	must(t, os.MkdirAll(filepath.Join(bp.b.TargetFolder, path), 0755))
}

func (bp *bowlerPreparator) symlink(path string, dest string) {
	t := bp.b.t

	targetPath := filepath.Join(bp.b.TargetFolder, path)
	must(t, os.MkdirAll(filepath.Dir(targetPath), 0755))
	must(t, os.Symlink(dest, targetPath))
}

func (bp *bowlerPreparator) file(path string, data []byte) {
	t := bp.b.t

	mode := os.FileMode(0644)

	targetPath := filepath.Join(bp.b.TargetFolder, path)
	must(t, os.MkdirAll(filepath.Dir(targetPath), 0755))
	must(t, ioutil.WriteFile(targetPath, data, mode))
}

// bowler simulator

type bowlerSimulatorMode int

const (
	bowlerSimulatorModeMakeReference bowlerSimulatorMode = 0
	bowlerSimulatorModeApply         bowlerSimulatorMode = 1
)

type bowlerSimulator struct {
	b    *bowler
	bowl bowl.Bowl
	mode bowlerSimulatorMode
}

func (bs *bowlerSimulator) patch(sourcePath string, data []byte) {
	t := bs.b.t

	switch bs.mode {
	case bowlerSimulatorModeMakeReference:
		must(t, func() error {
			refPath := filepath.Join(bs.b.RefFolder, sourcePath)
			must(t, os.MkdirAll(filepath.Dir(refPath), 0755))

			w, err := os.Create(refPath)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			_, err = w.Write(data)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			return nil
		}())
	case bowlerSimulatorModeApply:
		sourceIndex := findFile(t, bs.b.SourceContainer, sourcePath)

		w, err := bs.bowl.GetWriter(sourceIndex)
		defer func() {
			must(t, w.Close())
		}()

		_, err = w.Write(data)
		must(t, err)
	}
}

func (bs *bowlerSimulator) transpose(targetPath string, sourcePath string) {
	t := bs.b.t

	targetIndex := findFile(t, bs.b.TargetContainer, targetPath)

	switch bs.mode {
	case bowlerSimulatorModeMakeReference:
		must(t, func() error {
			r, err := bs.b.TargetPool.GetReader(targetIndex)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			refPath := filepath.Join(bs.b.RefFolder, sourcePath)
			must(t, os.MkdirAll(filepath.Dir(refPath), 0755))

			w, err := os.Create(refPath)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			_, err = io.Copy(w, r)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			return nil
		}())
	case bowlerSimulatorModeApply:
		sourceIndex := findFile(t, bs.b.SourceContainer, sourcePath)

		must(t, bs.bowl.Transpose(bowl.Transposition{
			TargetIndex: targetIndex,
			SourceIndex: sourceIndex,
		}))
	}
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

// must - it's a must!

func must(t *testing.T, err error) {
	if err != nil {
		if se, ok := err.(*errors.Error); ok {
			t.Logf("Full stack: %s", se.ErrorStack())
		}
		assert.NoError(t, err)
		t.FailNow()
	}
}
