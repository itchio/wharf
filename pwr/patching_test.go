package pwr

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/archiver"
	"github.com/itchio/wharf/wire"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type patchScenario struct {
	name                  string
	v1                    testDirSettings
	intermediate          *testDirSettings
	corruptions           *testDirSettings
	healedBytes           int64
	v2                    testDirSettings
	touchedFiles          int // files that were written to (not renamed) during apply
	noopFiles             int // files that were left as-is during apply
	movedFiles            int
	deletedFiles          int
	deletedSymlinks       int
	deletedDirs           int
	leftDirs              int  // folders that couldn't be deleted during apply (because of non-container files in them)
	extraTests            bool // run in-place patching, etc.
	testBrokenRename      bool // pretend os.Rename() doesn't work (it doesn't, sometimes, across partitions)
	unchanged             bool // if true, before folder validates, so don't check that
	ineffectiveCorruption bool // if true, before folder validates, so don't check that
	testVet               bool // test that vetting rejections do reject
	partitions            int
}

const largeAmount int64 = 16

// This is more of an integration test, it hits a lot of statements
func Test_ChangeOne(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "change one",
		touchedFiles: 1,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x1, size: BlockSize*11 + 14},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: BlockSize*11 + 14},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x1, size: BlockSize*17 + 14},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
		healedBytes: BlockSize*17 + 14,
		extraTests:  true,
		testVet:     true,
	})
}

func Test_OneBecameShort(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "one became short",
		touchedFiles: 1,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize - 17},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize},
					testDirChunk{seed: 0x222, size: 17},
				}},
			},
		},
		healedBytes: BlockSize + 17,
		extraTests:  true,
	})
}

func Test_EarlySmallWound(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "early small wound",
		touchedFiles: 1,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize * 16},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize},
					testDirChunk{seed: 0x222, size: 1},
					testDirChunk{seed: 0x111, size: BlockSize*15 - 1},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize * 16},
					testDirChunk{seed: 0x333, size: BlockSize * 16},
				}},
			},
		},
		healedBytes: BlockSize * (16 + 16),
		extraTests:  true,
	})
}

func Test_ChangeOneInMiddle(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "change one in the middle",
		touchedFiles: 1,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize*12 + 1},
					testDirChunk{seed: 0x222, size: BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: BlockSize*12 + 4},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize*12 + 1},
					testDirChunk{seed: 0x222, size: BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: BlockSize * 12},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: BlockSize*12 + 1},
					testDirChunk{seed: 0x444, size: BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: BlockSize*12 + 4},
				}},
			},
		},
		healedBytes: (BlockSize*12 + 1) + (BlockSize*12 + 3) + (BlockSize*12 + 4),
		extraTests:  true,
	})
}

func Test_AddOneRemoveOne(t *testing.T) {
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
}

func Test_RenameOne(t *testing.T) {
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
}

func Test_DeleteFolderOneGenerated(t *testing.T) {
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
}

func Test_Move4Files(t *testing.T) {
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
		testBrokenRename: true,
	})
}

func Test_Move4FilesIntoSubdir(t *testing.T) {
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
		testBrokenRename: true,
	})
}

func Test_OneFileIsDupedTwice(t *testing.T) {
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
		testBrokenRename: true,
	})
}

func Test_OneFileRenamedAndDuped(t *testing.T) {
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
		testBrokenRename: true,
	})
}

func Test_FourLargeUnchanged(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "four large unchanged",
		touchedFiles: 0,
		deletedFiles: 0,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: BlockSize*largeAmount + 17},
			},
		},
		healedBytes:           0, // we don't heal what we don't patch
		extraTests:            true,
		unchanged:             true,
		ineffectiveCorruption: true,
	})
}

func Test_FourLargeTwoSwap(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "four large, two swap",
		touchedFiles: 0,
		deletedFiles: 0,
		movedFiles:   2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x44, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x33, size: BlockSize*largeAmount + 17},
			},
		},
		healedBytes:           0, // we don't heal what we don't patch
		extraTests:            true,
		ineffectiveCorruption: true,
	})
}

func Test_FourLargeTwoSwapAndDupeA(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "four large, two swap + duplicate (option A)",
		touchedFiles: 2,
		deletedFiles: 0,
		movedFiles:   2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x22, size: BlockSize*largeAmount + 17},
			},
		},
		healedBytes:           0, // we don't heal what we don't patch
		extraTests:            true,
		ineffectiveCorruption: true,
	})
}

func Test_FourLargeTwoSwapAndDupeB(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "four large, two swap + duplicate (option B)",
		touchedFiles: 2,
		deletedFiles: 0,
		movedFiles:   2,
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x11, size: BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x11, size: BlockSize*largeAmount + 17},
			},
		},
		healedBytes:           0, // we don't heal what we don't patch
		extraTests:            true,
		ineffectiveCorruption: true,
	})
}

func Test_SymlinksAddedByPatch(t *testing.T) {
	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are added by patch",
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file", seed: 0x1},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file", seed: 0x1},
					{path: "dir1/link", dest: "file"},
				},
			},
		})
	}
}

func Test_SymlinksChangedByPatch(t *testing.T) {
	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are changed by patch",
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file1", seed: 0x1},
					{path: "dir1/file2", seed: 0x2},
					{path: "dir1/link", dest: "file1"},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file1", seed: 0x1},
					{path: "dir1/file2", seed: 0x2},
					{path: "dir1/link", dest: "file2"},
				},
			},
		})
	}
}

func Test_SymlinksRemovedByPatch(t *testing.T) {
	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name:            "symlinks are removed by patch",
			deletedSymlinks: 1,
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file", seed: 0x1},
					{path: "dir1/link", dest: "file"},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "dir1/file", seed: 0x1},
				},
			},
		})
	}
}

type SetupFunc func(actx *ApplyContext)

func runPatchingScenario(t *testing.T, scenario patchScenario) {
	log := t.Logf

	mainDir, err := ioutil.TempDir("", "patch-cycle")
	must(t, err)
	must(t, os.MkdirAll(mainDir, 0755))
	defer os.RemoveAll(mainDir)

	v1 := filepath.Join(mainDir, "v1")
	makeTestDir(t, v1, scenario.v1)

	v2 := filepath.Join(mainDir, "v2")
	makeTestDir(t, v2, scenario.v2)

	healPathBase := filepath.Join(mainDir, "v2.zip")

	func() {
		fw, err := os.Create(healPathBase)
		must(t, err)

		_, err = archiver.CompressZip(fw, v2, nil)
		must(t, err)
	}()

	getHealPath := func() string {
		return fmt.Sprintf("archive,%s", healPathBase)
	}

	compression := &CompressionSettings{}
	compression.Algorithm = CompressionAlgorithm_NONE

	sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
	must(t, err)

	consumer := &state.Consumer{}
	patchBuffer := new(bytes.Buffer)
	signatureBuffer := new(bytes.Buffer)

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, &tlc.WalkOpts{})
		must(t, dErr)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, dErr := ComputeSignature(context.Background(), targetContainer, targetPool, consumer)
		must(t, dErr)

		pool := fspool.New(sourceContainer, v2)

		dctx := &DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		must(t, dctx.WritePatch(context.Background(), patchBuffer, signatureBuffer))
	}()

	v1Before := filepath.Join(mainDir, "v1Before")
	cpDir(t, v1, v1Before)

	v1After := filepath.Join(mainDir, "v1After")

	woundsPath := filepath.Join(mainDir, "wounds.pww")

	if scenario.extraTests {
		sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
		_, sigErr := sigReader.Resume(nil)
		must(t, sigErr)

		signature, sErr := ReadSignature(context.Background(), sigReader)
		must(t, sErr)

		if !scenario.unchanged {
			log("Making sure before-path folder doesn't validate")
			assert.Error(t, AssertValid(v1Before, signature))
		}

		runExtraTest := func(setup SetupFunc) error {
			must(t, os.RemoveAll(woundsPath))
			must(t, os.RemoveAll(v1Before))
			cpDir(t, v1, v1Before)

			actx := &ApplyContext{
				TargetPath: v1Before,
				OutputPath: v1Before,

				InPlace:  true,
				Consumer: consumer,
			}
			if setup != nil {
				setup(actx)
			}

			patchReader := seeksource.FromBytes(patchBuffer.Bytes())
			_, sErr := patchReader.Resume(nil)
			must(t, sErr)

			aErr := actx.ApplyPatch(patchReader)
			if aErr != nil {
				return aErr
			}

			if actx.Signature == nil {
				vErr := AssertValid(v1Before, signature)
				if vErr != nil {
					return vErr
				}
			}

			return nil
		}

		func() {
			if !scenario.testVet {
				return
			}

			log("In-place with vet rejection")
			var NotVettingError = errors.New("not vetting this")
			pErr := runExtraTest(func(actx *ApplyContext) {
				actx.VetApply = func(actx *ApplyContext) error {
					return NotVettingError
				}
			})
			assert.Error(t, pErr)
			assert.Equal(t, NotVettingError, errors.Cause(pErr))
		}()

		func() {
			log("In-place with signature (f4ilfast, no corruptions)")
			must(t, runExtraTest(func(actx *ApplyContext) {
				actx.Signature = signature
			}))
		}()

		func() {
			if scenario.ineffectiveCorruption {
				return
			}

			log("In-place with signature (f4ilfast, with corruptions)")
			testErr := runExtraTest(func(actx *ApplyContext) {
				actx.Signature = signature
				makeTestDir(t, v1Before, *scenario.corruptions)
			})

			assert.Error(t, testErr)
		}()

		func() {
			log("In-place with signature (wounds, no corruptions)")
			must(t, runExtraTest(func(actx *ApplyContext) {
				actx.Signature = signature
				actx.WoundsPath = woundsPath
			}))

			_, sErr := os.Lstat(woundsPath)
			assert.Error(t, sErr)
			assert.True(t, os.IsNotExist(sErr))
		}()

		func() {
			if scenario.ineffectiveCorruption {
				return
			}

			log("In-place with signature (wounds, corruptions)")
			must(t, runExtraTest(func(actx *ApplyContext) {
				actx.Signature = signature
				actx.WoundsPath = woundsPath
				makeTestDir(t, v1Before, *scenario.corruptions)
			}))

			_, sErr := os.Lstat(woundsPath)
			must(t, sErr)
		}()

		func() {
			log("In-place with signature (heal, no corruptions)")

			must(t, runExtraTest(func(actx *ApplyContext) {
				actx.Signature = signature
				actx.HealPath = getHealPath()
			}))
		}()

		func() {
			if scenario.ineffectiveCorruption {
				return
			}

			log("In-place with signature (heal, corruptions)")
			var ctx *ApplyContext

			must(t, runExtraTest(func(actx *ApplyContext) {
				ctx = actx
				actx.Signature = signature
				actx.HealPath = getHealPath()
				makeTestDir(t, v1Before, *scenario.corruptions)
			}))

			must(t, AssertValid(v2, signature))

			healer, ok := ctx.WoundsConsumer.(Healer)
			assert.True(t, ok)

			assert.EqualValues(t, healer.TotalHealed(), scenario.healedBytes)
		}()
	}

	log("Applying to other directory, with separate check")
	must(t, os.RemoveAll(v1Before))
	cpDir(t, v1, v1Before)

	func() {
		actx := &ApplyContext{
			TargetPath: v1Before,
			OutputPath: v1After,

			Consumer: consumer,
		}

		patchReader := seeksource.FromBytes(patchBuffer.Bytes())
		_, sErr := patchReader.Resume(nil)
		must(t, sErr)

		aErr := actx.ApplyPatch(patchReader)
		must(t, aErr)

		assert.Equal(t, 0, actx.Stats.DeletedFiles, "deleted files (other dir)")
		assert.Equal(t, 0, actx.Stats.DeletedDirs, "deleted dirs (other dir)")
		assert.Equal(t, 0, actx.Stats.DeletedSymlinks, "deleted symlinks (other dir)")
		assert.Equal(t, 0, actx.Stats.MovedFiles, "moved files (other dir)")
		assert.Equal(t, len(sourceContainer.Files), actx.Stats.TouchedFiles, "touched files (other dir)")
		assert.Equal(t, 0, actx.Stats.NoopFiles, "noop files (other dir)")

		sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
		_, sigErr := sigReader.Resume(nil)
		must(t, sigErr)

		signature, sErr := ReadSignature(context.Background(), sigReader)
		must(t, sErr)

		must(t, AssertValid(v1After, signature))
	}()

	log("Applying in-place")

	testAll := func(setup SetupFunc) {
		must(t, os.RemoveAll(v1After))
		must(t, os.RemoveAll(v1Before))
		cpDir(t, v1, v1Before)

		func() {
			actx := &ApplyContext{
				TargetPath: v1Before,
				OutputPath: v1Before,

				InPlace: true,

				Consumer: consumer,
			}
			if setup != nil {
				setup(actx)
			}

			patchReader := seeksource.FromBytes(patchBuffer.Bytes())
			_, sErr := patchReader.Resume(nil)
			must(t, sErr)

			aErr := actx.ApplyPatch(patchReader)
			must(t, aErr)

			assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles, "deleted files (in-place)")
			assert.Equal(t, scenario.deletedSymlinks, actx.Stats.DeletedSymlinks, "deleted symlinks (in-place)")
			assert.Equal(t, scenario.deletedDirs+scenario.leftDirs, actx.Stats.DeletedDirs, "deleted dirs (in-place)")
			assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles, "touched files (in-place)")
			assert.Equal(t, scenario.movedFiles, actx.Stats.MovedFiles, "moved files (in-place)")
			assert.Equal(t, len(sourceContainer.Files)-scenario.touchedFiles-scenario.movedFiles, actx.Stats.NoopFiles, "noop files (in-place)")

			sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
			_, sigErr := sigReader.Resume(nil)
			must(t, sigErr)

			signature, sErr := ReadSignature(context.Background(), sigReader)
			must(t, sErr)

			must(t, AssertValid(v1Before, signature))
		}()

		if scenario.intermediate != nil {
			log("Applying in-place with %d intermediate files", len(scenario.intermediate.entries))

			must(t, os.RemoveAll(v1After))
			must(t, os.RemoveAll(v1Before))
			cpDir(t, v1, v1Before)

			makeTestDir(t, v1Before, *scenario.intermediate)

			func() {
				actx := &ApplyContext{
					TargetPath: v1Before,
					OutputPath: v1Before,

					InPlace: true,

					Consumer: consumer,
				}
				if setup != nil {
					setup(actx)
				}

				patchReader := seeksource.FromBytes(patchBuffer.Bytes())
				_, rErr := patchReader.Resume(nil)
				must(t, rErr)

				aErr := actx.ApplyPatch(patchReader)
				must(t, aErr)

				assert.Equal(t, scenario.deletedFiles, actx.Stats.DeletedFiles, "deleted files (in-place w/intermediate)")
				assert.Equal(t, scenario.deletedDirs, actx.Stats.DeletedDirs, "deleted dirs (in-place w/intermediate)")
				assert.Equal(t, scenario.deletedSymlinks, actx.Stats.DeletedSymlinks, "deleted symlinks (in-place w/intermediate)")
				assert.Equal(t, scenario.touchedFiles, actx.Stats.TouchedFiles, "touched files (in-place w/intermediate)")
				assert.Equal(t, scenario.noopFiles, actx.Stats.NoopFiles, "noop files (in-place w/intermediate)")
				assert.Equal(t, scenario.leftDirs, actx.Stats.LeftDirs, "left dirs (in-place w/intermediate)")

				sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
				_, sigErr := sigReader.Resume(nil)
				must(t, sigErr)

				signature, sErr := ReadSignature(context.Background(), sigReader)
				must(t, sErr)

				must(t, AssertValid(v1Before, signature))
			}()
		}
	}

	testAll(nil)

	if scenario.testBrokenRename {
		testAll(func(actx *ApplyContext) {
			actx.debugBrokenRename = true
		})
	}
}

type CookPatchOperator func(wctx *wire.WriteContext)

func cookPatch(t *testing.T, op CookPatchOperator) *bytes.Buffer {
	buffer := new(bytes.Buffer)
	wctx := wire.NewWriteContext(buffer)
	op(wctx)

	return buffer
}

func Test_WrongMagicPatch(t *testing.T) {
	patch := cookPatch(t, func(wctx *wire.WriteContext) {
		ELFMagic := int32(0x7f454c46)
		must(t, wctx.WriteMagic(ELFMagic))
	})

	actx := ApplyContext{
		Consumer: &state.Consumer{},
	}

	patchReader := seeksource.FromBytes(patch.Bytes())
	_, rErr := patchReader.Resume(nil)
	must(t, rErr)

	err := actx.ApplyPatch(patchReader)
	assert.Error(t, err)
}

func Test_MissingHeaderPatch(t *testing.T) {
	patch := cookPatch(t, func(wctx *wire.WriteContext) {
		must(t, wctx.WriteMagic(PatchMagic))
	})

	actx := ApplyContext{
		Consumer: &state.Consumer{},
	}

	patchReader := seeksource.FromBytes(patch.Bytes())
	_, rErr := patchReader.Resume(nil)
	must(t, rErr)

	err := actx.ApplyPatch(patchReader)
	assert.Error(t, err)
}

func Test_OnlyHeaderPatch(t *testing.T) {
	patch := cookPatch(t, func(wctx *wire.WriteContext) {
		must(t, wctx.WriteMagic(PatchMagic))
		patchHeader := &PatchHeader{}
		must(t, wctx.WriteMessage(patchHeader))
	})

	actx := ApplyContext{
		Consumer: &state.Consumer{},
	}

	patchReader := seeksource.FromBytes(patch.Bytes())
	_, rErr := patchReader.Resume(nil)
	must(t, rErr)

	err := actx.ApplyPatch(patchReader)
	assert.Error(t, err)
}

func must(t *testing.T, err error) {
	if err != nil {
		assert.NoError(t, err)
		t.FailNow()
	}
}
