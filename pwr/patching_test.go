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
	name            string
	v1              testDirSettings
	intermediate    *testDirSettings
	v2              testDirSettings
	touchedFiles    int // files that were written to (not renamed) during apply
	noopFiles       int // files that were left as-is during apply
	movedFiles      int
	deletedFiles    int
	deletedSymlinks int
	deletedDirs     int
	leftDirs        int // folders that couldn't be deleted during apply (because of non-container files in them)
	testVet         bool
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
		testVet: true,
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
		touchedFiles: 0,
		deletedFiles: 0,
		movedFiles:   1,
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
		noopFiles:    1,
		touchedFiles: 1, // the new one
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
				{path: "dir3/subdir/subdir/file-2", seed: 0x289, size: BlockSize*3 + 12},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "move 4 files",
		touchedFiles: 0,
		movedFiles:   4,
		deletedFiles: 0,
		deletedDirs:  1,
		leftDirs:     2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "old/file-1", seed: 0x111},
				{path: "old/subdir/file-1", seed: 0x222},
				{path: "old/subdir/file-2", seed: 0x333},
				{path: "old/subdir/subdir/file-4", seed: 0x444},
			},
		},
		intermediate: &testDirSettings{
			entries: []testDirEntry{
				{path: "old/subdir/file-1-generated", seed: 0x999, size: BlockSize * 3},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "new/file-1", seed: 0x111},
				{path: "new/subdir/file-1", seed: 0x222},
				{path: "new/subdir/file-2", seed: 0x333},
				{path: "new/subdir/subdir/file-4", seed: 0x444},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "move 4 files into a subdirectory",
		touchedFiles: 0,
		movedFiles:   4,
		deletedFiles: 0,
		leftDirs:     1, // old/subdir
		deletedDirs:  1,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "old/file-1", seed: 0x1},
				{path: "old/subdir/file-1", seed: 0x2},
				{path: "old/subdir/file-2", seed: 0x3},
				{path: "old/subdir/subdir/file-4", seed: 0x4},
			},
		},
		intermediate: &testDirSettings{
			entries: []testDirEntry{
				{path: "old/subdir/file-1-generated", seed: 0x999, size: BlockSize * 3},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "old/new/file-1", seed: 0x1},
				{path: "old/new/subdir/file-1", seed: 0x2},
				{path: "old/new/subdir/file-2", seed: 0x3},
				{path: "old/new/subdir/subdir/file-4", seed: 0x4},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "one file is duplicated twice",
		touchedFiles: 2,
		noopFiles:    1,
		movedFiles:   0,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/file-1", seed: 0x1},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/file-1", seed: 0x1},
				{path: "dir2/file-1", seed: 0x1},
				{path: "dir2/file-1bis", seed: 0x1},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name:         "one file is renamed + duplicated twice",
		touchedFiles: 2,
		movedFiles:   1,
		deletedFiles: 0,
		deletedDirs:  1,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/file-1", seed: 0x1},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir2/file-1", seed: 0x1},
				{path: "dir3/file-1", seed: 0x1},
				{path: "dir3/file-1bis", seed: 0x1},
			},
		},
	})
}

func runPatchingScenario(t *testing.T, scenario patchScenario) {
	log := func(format string, args ...interface{}) {
		t.Logf("[%s] %s", scenario.name, fmt.Sprintf(format, args...))
	}
	log("Scenario start")

	mainDir, err := ioutil.TempDir("", "patch-cycle")
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

			assert.Equal(t, 0, actx.Stats.DeletedFiles, "deleted files (no vet)")
			assert.Equal(t, 0, actx.Stats.DeletedDirs, "deleted dirs (no vet)")
			assert.Equal(t, 0, actx.Stats.TouchedFiles, "touched files (no vet)")
			assert.Equal(t, 0, actx.Stats.NoopFiles, "noop files (no vet)")
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

		assert.Equal(t, 0, actx.Stats.DeletedFiles, "deleted files (other dir)")
		assert.Equal(t, 0, actx.Stats.DeletedDirs, "deleted dirs (other dir)")
		assert.Equal(t, 0, actx.Stats.DeletedSymlinks, "deleted symlinks (other dir)")
		assert.Equal(t, 0, actx.Stats.MovedFiles, "moved files (other dir)")
		assert.Equal(t, len(sourceContainer.Files), actx.Stats.TouchedFiles, "touched files (other dir)")
		assert.Equal(t, 0, actx.Stats.NoopFiles, "noop files (other dir)")

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

		assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles, "deleted files (in-place)")
		assert.Equal(t, scenario.deletedSymlinks, actx.Stats.DeletedSymlinks, "deleted symlinks (in-place)")
		assert.Equal(t, scenario.deletedDirs+scenario.leftDirs, actx.Stats.DeletedDirs, "deleted dirs (in-place)")
		assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles, "touched files (in-place)")
		assert.Equal(t, scenario.movedFiles, actx.Stats.MovedFiles, "moved files files (in-place)")
		assert.Equal(t, len(sourceContainer.Files)-scenario.touchedFiles-scenario.movedFiles, actx.Stats.NoopFiles, "noop files (in-place)")

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

			assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles, "deleted files (in-place w/intermediate)")
			assert.Equal(t, scenario.deletedDirs, actx.Stats.DeletedDirs, "deleted dirs (in-place w/intermediate)")
			assert.Equal(t, scenario.deletedSymlinks, actx.Stats.DeletedSymlinks, "deleted symlinks (in-place w/intermediate)")
			assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles, "touched files (in-place w/intermediate)")
			assert.Equal(t, scenario.noopFiles, actx.Stats.NoopFiles, "noop files (in-place w/intermediate)")
			assert.Equal(t, scenario.leftDirs, actx.Stats.LeftDirs, "left dirs (in-place w/intermediate)")

			signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
			assert.Nil(t, sErr)

			assert.Nil(t, AssertValid(v1Before, signature))
		}()
	}
}
