package bowl_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/screw"
	"github.com/itchio/wharf/archiver"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func ditto(dst string, src string) error {
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return errors.WithStack(err)
		}

		dstPath := filepath.Join(dst, relPath)

		switch {
		case info.IsDir():
			err = screw.MkdirAll(dstPath, info.Mode())
			if err != nil {
				return errors.WithStack(err)
			}
		case info.Mode()&os.ModeSymlink > 0:
			return errors.New("don't know how to ditto symlink")
		default:
			err = func() error {
				r, err := screw.Open(path)
				if err != nil {
					return errors.WithStack(err)
				}
				defer r.Close()

				w, err := screw.OpenFile(dstPath, os.O_CREATE, info.Mode())
				if err != nil {
					return errors.WithStack(err)
				}
				defer w.Close()

				_, err = io.Copy(w, r)
				if err != nil {
					return errors.WithStack(err)
				}
				return nil
			}()
			if err != nil {
				return errors.WithStack(err)
			}
		}

		return nil
	})

	if err != nil {
		return errors.WithStack(err)
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
	TargetPool      lake.Pool

	TargetFolder string
	RefFolder    string
	FreshFolder  string
}

type bowlMode int

const (
	bowlModeNoop    bowlMode = 0
	bowlModeFresh   bowlMode = 1
	bowlModeInPlace bowlMode = 2
	bowlModeZip     bowlMode = 3
)

type bowlerParams struct {
	makeTarget func(p *bowlerPreparator)
	makeBowl   func(p *makeBowlParams) (bowl.Bowl, bowlMode)
	apply      func(s *bowlerSimulator)
}

type makeBowlParams struct {
	TargetContainer *tlc.Container
	SourceContainer *tlc.Container
	TargetPool      lake.Pool
	TargetFolder    string
	FreshFolder     string

	ZipFilePath string
	Cleanup     func()
}

func runBowler(t *testing.T, params *bowlerParams) {
	b := &bowler{
		t: t,
	}

	targetFolder, err := os.MkdirTemp("", "bowler-target")
	must(t, err)
	defer screw.RemoveAll(targetFolder)
	b.TargetFolder = targetFolder

	refFolder, err := os.MkdirTemp("", "bowler-reference")
	must(t, err)
	defer screw.RemoveAll(refFolder)
	b.RefFolder = refFolder

	freshFolder, err := os.MkdirTemp("", "bowler-fresh")
	must(t, err)
	defer screw.RemoveAll(freshFolder)
	b.FreshFolder = freshFolder

	// fill up our target folder + target container
	b.TargetContainer = &tlc.Container{}
	bp := &bowlerPreparator{
		b: b,
	}
	params.makeTarget(bp)

	targetContainer, err := tlc.WalkDir(targetFolder, tlc.WalkOpts{})
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

	sourceContainer, err := tlc.WalkDir(refFolder, tlc.WalkOpts{})
	must(t, err)
	b.SourceContainer = sourceContainer

	// now create the bowl
	mbp := &makeBowlParams{
		TargetContainer: b.TargetContainer,
		SourceContainer: b.SourceContainer,
		TargetPool:      targetPool,
		TargetFolder:    targetFolder,
		FreshFolder:     freshFolder,
	}
	bowl, bowlmode := params.makeBowl(mbp)

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

	if mbp.Cleanup != nil {
		mbp.Cleanup()
	}

	// and now check!
	var outFolder = ""
	switch bowlmode {
	case bowlModeFresh:
		outFolder = freshFolder
	case bowlModeInPlace:
		outFolder = targetFolder
	case bowlModeZip:
		outFolder = freshFolder
		_, err = archiver.ExtractPath(mbp.ZipFilePath, outFolder, archiver.ExtractSettings{
			Consumer: &state.Consumer{},
		})
		must(t, err)
		must(t, screw.Remove(mbp.ZipFilePath))
	}

	if outFolder != "" {
		refContainer, err := tlc.WalkDir(refFolder, tlc.WalkOpts{})
		must(t, err)
		outContainer, err := tlc.WalkDir(outFolder, tlc.WalkOpts{})
		must(t, err)

		must(t, refContainer.EnsureEqual(outContainer))
		must(t, outContainer.EnsureEqual(refContainer))

		refPool := fspool.New(refContainer, refFolder)
		outPool := fspool.New(refContainer, outFolder)

		for index := range refContainer.Files {
			refReader, err := refPool.GetReader(int64(index))
			must(t, err)
			refBytes, err := io.ReadAll(refReader)
			must(t, err)

			outReader, err := outPool.GetReader(int64(index))
			must(t, err)
			outBytes, err := io.ReadAll(outReader)
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

	must(t, screw.MkdirAll(filepath.Join(bp.b.TargetFolder, path), 0755))
}

func (bp *bowlerPreparator) symlink(path string, dest string) {
	t := bp.b.t

	targetPath := filepath.Join(bp.b.TargetFolder, path)
	must(t, screw.MkdirAll(filepath.Dir(targetPath), 0755))
	must(t, screw.Symlink(dest, targetPath))
}

func (bp *bowlerPreparator) file(path string, data []byte) {
	t := bp.b.t

	mode := os.FileMode(0644)

	targetPath := filepath.Join(bp.b.TargetFolder, path)
	must(t, screw.MkdirAll(filepath.Dir(targetPath), 0755))
	must(t, os.WriteFile(targetPath, data, mode))
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
			must(t, screw.MkdirAll(filepath.Dir(refPath), 0755))

			w, err := screw.Create(refPath)
			if err != nil {
				return errors.WithStack(err)
			}

			_, err = w.Write(data)
			if err != nil {
				return errors.WithStack(err)
			}

			return nil
		}())
	case bowlerSimulatorModeApply:
		sourceIndex := findFile(t, bs.b.SourceContainer, sourcePath)

		w, err := bs.bowl.GetWriter(sourceIndex)
		defer func() {
			must(t, w.Close())
		}()

		_, err = w.Resume(nil)
		must(t, err)

		_, err = w.Write(data)
		must(t, err)

		err = w.Finalize()
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
				return errors.WithStack(err)
			}

			refPath := filepath.Join(bs.b.RefFolder, sourcePath)
			must(t, screw.MkdirAll(filepath.Dir(refPath), 0755))

			w, err := screw.Create(refPath)
			if err != nil {
				return errors.WithStack(err)
			}

			_, err = io.Copy(w, r)
			if err != nil {
				return errors.WithStack(err)
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
		assert.NoError(t, err)
		t.FailNow()
	}
}
