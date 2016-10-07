package pwr

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
)

type patchScenario struct {
	name         string
	v1           testDirSettings
	intermediate *testDirSettings
	v2           testDirSettings
	touchedFiles int // files that were touched as a result of applying the patch
	deletedFiles int // files that were deleted as a result of applying the patch
	deletedDirs  int // directories that were deleted as a result of applying the patch
	leftDirs     int // folders that couldn't be deleted
	testVet      bool
}

// This is more of an integration test, it hits a lot of statements
func Test_PatchCycle(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "change one",
		touchedFiles: 1,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x1},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x111},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "add one, remove one",
		touchedFiles: 1,
		deletedFiles: 1,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/file-1", seed: 0x2},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "rename one",
		touchedFiles: 1,
		deletedFiles: 1,
		deletedDirs:  2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/subdir/file-1", seed: 0x2, size: BlockSize*12 + 13},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir3/subdir/subdir/file-2", seed: 0x2, size: BlockSize*12 + 13},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "delete folder, one generated",
		touchedFiles: 1,
		deletedFiles: 1,
		leftDirs:     2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/subdir/file-1", seed: 0x2, size: BlockSize*12 + 13},
			},
		},
		intermediate: &testDirSettings{
			entries: []testDirEntry{
				{path: "dir2/subdir/file-1-generated", seed: 0x999, size: BlockSize * 3},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir3/subdir/subdir/file-2", seed: 0x2, size: BlockSize*12 + 13},
			},
		},
	})
}

func runPatchingScenario(t *testing.T, scenario patchScenario) {
	log := func(format string, args ...interface{}) {
		t.Logf("[%s] %s", scenario.name, fmt.Sprintf(format, args...))
	}
	log("Scenario start")

	mainDir, err := ioutil.TempDir("", fmt.Sprintf("patch-cycle-%s", scenario.name))
	assert.Nil(t, err)
	assert.Nil(t, os.MkdirAll(mainDir, 0755))
	defer os.RemoveAll(mainDir)

	v1 := filepath.Join(mainDir, "v1")
	makeTestDir(t, v1, scenario.v1)

	v2 := filepath.Join(mainDir, "v2")
	makeTestDir(t, v2, scenario.v2)

	compression := &CompressionSettings{}
	compression.Algorithm = CompressionAlgorithm_NONE

	sourceContainer, err := tlc.WalkAny(v2, nil)
	assert.Nil(t, err)

	consumer := &state.Consumer{}
	patchBuffer := new(bytes.Buffer)
	signatureBuffer := new(bytes.Buffer)

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, nil)
		assert.Nil(t, dErr)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, dErr := ComputeSignature(targetContainer, targetPool, consumer)
		assert.Nil(t, dErr)

		pool := fspool.New(sourceContainer, v2)

		dctx := &DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		assert.Nil(t, dctx.WritePatch(patchBuffer, signatureBuffer))
	}()

	v1Before := filepath.Join(mainDir, "v1Before")
	cpDir(t, v1, v1Before)

	v1After := filepath.Join(mainDir, "v1After")

	if scenario.testVet {
		log("Refusing to vet")

		assert.Nil(t, os.RemoveAll(v1Before))
		cpDir(t, v1, v1Before)

		var NotVettingError = errors.New("not vetting this")

		func() {
			actx := &ApplyContext{
				TargetPath: v1Before,
				OutputPath: v1Before,

				InPlace: true,
				VetApply: func(actx *ApplyContext) error {
					return NotVettingError
				},

				Consumer: consumer,
			}

			patchReader := bytes.NewReader(patchBuffer.Bytes())

			aErr := actx.ApplyPatch(patchReader)
			assert.NotNil(t, aErr)
			assert.True(t, errors.Is(aErr, NotVettingError))

			assert.Equal(t, 0, actx.Stats.DeletedFiles)
			assert.Equal(t, 0, actx.Stats.DeletedDirs)
			assert.Equal(t, 0, actx.Stats.TouchedFiles)
			assert.Equal(t, 0, actx.Stats.NoopFiles)
		}()
	}

	log("Applying to other directory, with separate check")

	func() {
		actx := &ApplyContext{
			TargetPath: v1Before,
			OutputPath: v1After,

			Consumer: consumer,
		}

		patchReader := bytes.NewReader(patchBuffer.Bytes())

		aErr := actx.ApplyPatch(patchReader)
		assert.Nil(t, aErr)

		assert.Equal(t, 0, actx.Stats.DeletedFiles)
		assert.Equal(t, 0, actx.Stats.DeletedDirs)
		assert.Equal(t, len(sourceContainer.Files), actx.Stats.TouchedFiles)
		assert.Equal(t, 0, actx.Stats.NoopFiles)

		signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
		assert.Nil(t, sErr)

		assert.Nil(t, AssertValid(v1After, signature))
	}()

	log("Applying in-place")

	assert.Nil(t, os.RemoveAll(v1After))
	assert.Nil(t, os.RemoveAll(v1Before))
	cpDir(t, v1, v1Before)

	func() {
		actx := &ApplyContext{
			TargetPath: v1Before,
			OutputPath: v1Before,

			InPlace: true,

			Consumer: consumer,
		}

		patchReader := bytes.NewReader(patchBuffer.Bytes())

		aErr := actx.ApplyPatch(patchReader)
		assert.Nil(t, aErr)

		assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles)
		assert.Equal(t, scenario.deletedDirs+scenario.leftDirs, actx.Stats.DeletedDirs)
		assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles)
		assert.Equal(t, len(sourceContainer.Files)-scenario.touchedFiles, actx.Stats.NoopFiles)

		signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
		assert.Nil(t, sErr)

		assert.Nil(t, AssertValid(v1Before, signature))
	}()

	if scenario.intermediate != nil {
		log("Applying in-place with %d intermediate files", len(scenario.intermediate.entries))

		assert.Nil(t, os.RemoveAll(v1After))
		assert.Nil(t, os.RemoveAll(v1Before))
		cpDir(t, v1, v1Before)

		makeTestDir(t, v1Before, *scenario.intermediate)

		func() {
			actx := &ApplyContext{
				TargetPath: v1Before,
				OutputPath: v1Before,

				InPlace: true,

				Consumer: consumer,
			}

			patchReader := bytes.NewReader(patchBuffer.Bytes())

			aErr := actx.ApplyPatch(patchReader)
			assert.Nil(t, aErr)

			assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles)
			assert.Equal(t, scenario.deletedDirs, actx.Stats.DeletedDirs)
			assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles)
			assert.Equal(t, len(sourceContainer.Files)-scenario.touchedFiles, actx.Stats.NoopFiles)
			assert.Equal(t, scenario.leftDirs, actx.Stats.LeftDirs)

			signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
			assert.Nil(t, sErr)

			assert.Nil(t, AssertValid(v1Before, signature))
		}()
	}
}
