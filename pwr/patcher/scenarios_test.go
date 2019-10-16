package patcher_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/archiver"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/wtest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_Scenarios(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name: "change one",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x1, size: pwr.BlockSize*11 + 14},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: pwr.BlockSize*11 + 14},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x1, size: pwr.BlockSize*17 + 14},
				{path: "file-1", seed: 0x2},
				{path: "dir2/file-2", seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "one became short",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize - 17},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize},
					testDirChunk{seed: 0x222, size: 17},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "early small wound",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize * 16},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize},
					testDirChunk{seed: 0x222, size: 1},
					testDirChunk{seed: 0x111, size: pwr.BlockSize*15 - 1},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "short", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize * 16},
					testDirChunk{seed: 0x333, size: pwr.BlockSize * 16},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change one in the middle",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize*12 + 1},
					testDirChunk{seed: 0x222, size: pwr.BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: pwr.BlockSize*12 + 4},
				}},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize*12 + 1},
					testDirChunk{seed: 0x222, size: pwr.BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: pwr.BlockSize * 12},
				}},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", chunks: []testDirChunk{
					testDirChunk{seed: 0x111, size: pwr.BlockSize*12 + 1},
					testDirChunk{seed: 0x444, size: pwr.BlockSize*12 + 3},
					testDirChunk{seed: 0x333, size: pwr.BlockSize*12 + 4},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "add one, remove one",
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
		name: "rename one",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/subdir/file-1", seed: 0x2, size: pwr.BlockSize*12 + 13},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir3/subdir/subdir/file-2", seed: 0x2, size: pwr.BlockSize*12 + 13},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "delete folder, one generated",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir2/subdir/file-1", seed: 0x2, size: pwr.BlockSize*12 + 13},
			},
		},
		intermediate: &testDirSettings{
			entries: []testDirEntry{
				{path: "dir2/subdir/file-1-generated", seed: 0x999, size: pwr.BlockSize * 3},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "dir1/subdir/file-1", seed: 0x1},
				{path: "dir3/subdir/subdir/file-2", seed: 0x289, size: pwr.BlockSize*3 + 12},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "move 4 files",
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
				{path: "old/subdir/file-1-generated", seed: 0x999, size: pwr.BlockSize * 3},
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
		name: "move 4 files into a subdirectory",
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
				{path: "old/subdir/file-1-generated", seed: 0x999, size: pwr.BlockSize * 3},
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
		name: "one file is duplicated twice",
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
		name: "one file is renamed + duplicated twice",
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

	runPatchingScenario(t, patchScenario{
		name: "four large unchanged",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap + duplicate (option A)",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap + duplicate (option B)",
		v1: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x33, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x44, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x99, size: pwr.BlockSize*largeAmount + 17},
			},
		},
		v2: testDirSettings{
			entries: []testDirEntry{
				{path: "subdir/file-1", seed: 0x22, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-2", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-3", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
				{path: "subdir/file-4", seed: 0x11, size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

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

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are removed by patch",
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

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes normal file (itchio/itch#2315)",
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "test2.txt", seed: 0x1},
					{path: "test.txt", dest: "test2.txt"},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "test2.txt", seed: 0x1},
					{path: "test.txt", seed: 0x2},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes directory",
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "test2.txt", seed: 0x1},
					{path: "test.txt", dest: "test2.txt"},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "test2.txt", seed: 0x1},
					{path: "test.txt/woop", seed: 0x2},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes dangling",
			v1: testDirSettings{
				entries: []testDirEntry{
					{path: "test2.txt", seed: 0x1},
					{path: "test.txt", dest: "test2.txt"},
				},
			},
			v2: testDirSettings{
				entries: []testDirEntry{
					{path: "test.txt", dest: "test2.txt"},
				},
			},
		})
	}
}

func runPatchingScenario(t *testing.T, scenario patchScenario) {
	forwardScenario := *&scenario
	forwardScenario.name += "/forward"
	runSinglePatchingScenario(t, forwardScenario)

	backScenario := *&scenario
	backScenario.name += "/back"
	backScenario.v1, backScenario.v2 = backScenario.v2, backScenario.v1
	runSinglePatchingScenario(t, backScenario)
}

func runSinglePatchingScenario(t *testing.T, scenario patchScenario) {
	t.Run(scenario.name, func(t *testing.T) {
		log := t.Logf

		mainDir, err := ioutil.TempDir("", "patch-cycle")
		wtest.Must(t, err)
		defer os.RemoveAll(mainDir)

		v1 := filepath.Join(mainDir, "v1")
		makeTestDir(t, v1, scenario.v1)

		v2 := filepath.Join(mainDir, "v2")
		makeTestDir(t, v2, scenario.v2)

		v1zip := filepath.Join(mainDir, "v1.zip")
		v2zip := filepath.Join(mainDir, "v2.zip")

		func() {
			fw, err := os.Create(v1zip)
			wtest.Must(t, err)

			_, err = archiver.CompressZip(fw, v1, nil)
			wtest.Must(t, err)
		}()

		func() {
			fw, err := os.Create(v2zip)
			wtest.Must(t, err)

			_, err = archiver.CompressZip(fw, v2, nil)
			wtest.Must(t, err)
		}()

		consumer := &state.Consumer{
			OnMessage: func(level string, message string) {
				t.Logf("[%s] %s", level, message)
			},
		}

		assertValid := func(target string, signature *pwr.SignatureInfo) error {
			targetContainer, err := tlc.WalkAny(target, &tlc.WalkOpts{})
			wtest.Must(t, err)

			consumer.Debugf("===================================")
			consumer.Debugf("validating container:")
			targetContainer.Print(func(line string) {
				consumer.Debugf(line)
			})
			consumer.Debugf("===================================")

			vctx := &pwr.ValidatorContext{
				FailFast: true,
				Consumer: consumer,
			}

			return vctx.Validate(context.Background(), target, signature)
		}

		compression := &pwr.CompressionSettings{}
		compression.Algorithm = pwr.CompressionAlgorithm_BROTLI
		compression.Quality = 1

		targetContainer, err := tlc.WalkAny(v1, &tlc.WalkOpts{})
		wtest.Must(t, err)

		sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
		wtest.Must(t, err)

		consumer.Debugf("===================================")
		consumer.Debugf("v1 contents:")
		targetContainer.Print(func(line string) {
			consumer.Debugf(line)
		})
		consumer.Debugf("-----------------------------------")
		consumer.Debugf("v2 contents:")
		sourceContainer.Print(func(line string) {
			consumer.Debugf(line)
		})
		consumer.Debugf("===================================")

		patchBuffer := new(bytes.Buffer)
		optimizedPatchBuffer := new(bytes.Buffer)
		signatureBuffer := new(bytes.Buffer)
		var v1Sig *pwr.SignatureInfo
		var v2Sig *pwr.SignatureInfo

		func() {
			targetPool := fspool.New(targetContainer, v1)
			v1Hashes, err := pwr.ComputeSignature(context.Background(), targetContainer, targetPool, consumer)
			wtest.Must(t, err)

			v1Sig = &pwr.SignatureInfo{
				Container: targetContainer,
				Hashes:    v1Hashes,
			}

			pool := fspool.New(sourceContainer, v2)

			dctx := &pwr.DiffContext{
				Compression: compression,
				Consumer:    consumer,

				SourceContainer: sourceContainer,
				Pool:            pool,

				TargetContainer: targetContainer,
				TargetSignature: v1Hashes,
			}

			wtest.Must(t, dctx.WritePatch(context.Background(), patchBuffer, signatureBuffer))

			sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
			_, err = sigReader.Resume(nil)
			wtest.Must(t, err)

			v2Sig, err = pwr.ReadSignature(context.Background(), sigReader)
			wtest.Must(t, err)
		}()

		func() {
			rc := &pwr.RediffContext{
				TargetPool: fspool.New(targetContainer, v1),
				SourcePool: fspool.New(sourceContainer, v2),

				Consumer:              consumer,
				Compression:           compression,
				SuffixSortConcurrency: 0,
				Partitions:            2,
			}

			patchReader := seeksource.FromBytes(patchBuffer.Bytes())
			_, err := patchReader.Resume(nil)
			wtest.Must(t, err)

			err = rc.AnalyzePatch(patchReader)
			wtest.Must(t, err)

			_, err = patchReader.Resume(nil)
			wtest.Must(t, err)

			err = rc.OptimizePatch(patchReader, optimizedPatchBuffer)
			wtest.Must(t, err)
		}()

		log("    Naive patch: %s", united.FormatBytes(int64(patchBuffer.Len())))
		log("Optimized patch: %s", united.FormatBytes(int64(optimizedPatchBuffer.Len())))

		func() {
			outDir := filepath.Join(mainDir, "out")
			wtest.Must(t, os.MkdirAll(outDir, 0o755))

			stageDir := filepath.Join(mainDir, "stage")
			wtest.Must(t, os.MkdirAll(stageDir, 0o755))

			type Patch struct {
				Name   string
				Buffer *bytes.Buffer
			}
			patches := []Patch{
				Patch{
					Name:   "naive",
					Buffer: patchBuffer,
				},
				Patch{
					Name:   "optimized",
					Buffer: optimizedPatchBuffer,
				},
			}

			for _, patch := range patches {
				func() {
					log("Applying %s fresh (v1) -> (v2)", patch.Name)
					os.RemoveAll(outDir)
					wtest.Must(t, os.MkdirAll(outDir, 0o755))

					patchReader := seeksource.FromBytes(patch.Buffer.Bytes())
					_, err = patchReader.Resume(nil)
					wtest.Must(t, err)

					p, err := patcher.New(patchReader, consumer)
					wtest.Must(t, err)

					targetPool := fspool.New(p.GetTargetContainer(), v1)

					b, err := bowl.NewFreshBowl(bowl.FreshBowlParams{
						SourceContainer: p.GetSourceContainer(),
						TargetContainer: p.GetTargetContainer(),
						TargetPool:      targetPool,
						OutputFolder:    outDir,
					})
					wtest.Must(t, err)

					err = p.Resume(nil, targetPool, b)
					wtest.Must(t, err)

					wtest.Must(t, b.Commit())

					wtest.Must(t, assertValid(outDir, v2Sig))
					wtest.Must(t, pwr.AssertNoGhosts(outDir, v2Sig))
				}()

				if scenario.corruptions != nil {
					func() {
						log("Applying %s in-place (v1 corrupted) -> (v2)", patch.Name)
						os.RemoveAll(outDir)
						wtest.Must(t, os.MkdirAll(outDir, 0o755))
						cpDir(t, v1, outDir)
						makeTestDir(t, outDir, *scenario.corruptions)

						patchReader := seeksource.FromBytes(patch.Buffer.Bytes())
						_, err = patchReader.Resume(nil)
						wtest.Must(t, err)

						p, err := patcher.New(patchReader, consumer)
						wtest.Must(t, err)

						targetPool := fspool.New(p.GetTargetContainer(), outDir)

						b, err := bowl.NewOverlayBowl(bowl.OverlayBowlParams{
							SourceContainer: p.GetSourceContainer(),
							TargetContainer: p.GetTargetContainer(),
							StageFolder:     stageDir,
							OutputFolder:    outDir,
						})
						wtest.Must(t, err)

						err = func() error {
							err := p.Resume(nil, targetPool, b)
							if err != nil {
								return errors.WithMessage(err, "in patcher.Resume")
							}

							err = b.Commit()
							if err != nil {
								return errors.WithMessage(err, "in bowl.Commit")
							}

							err = assertValid(outDir, v2Sig)
							if err != nil {
								return errors.WithMessage(err, "in assertValid")
							}
							err = pwr.AssertNoGhosts(outDir, v2Sig)
							if err != nil {
								return errors.WithMessage(err, "in pwr.AssertNoGhosts")
							}

							return nil
						}()
						if err != nil {
							log("As expected, got an error: %+v", err)
						}
						if patch.Name == "naive" {
							// sometimes the optimized patches work anyway?
							assert.Error(t, err)
						}
					}()
				}

				func() {
					log("Applying %s in-place (v1) -> (v2)", patch.Name)
					os.RemoveAll(outDir)
					wtest.Must(t, os.MkdirAll(outDir, 0o755))
					cpDir(t, v1, outDir)

					patchReader := seeksource.FromBytes(patch.Buffer.Bytes())
					_, err = patchReader.Resume(nil)
					wtest.Must(t, err)

					p, err := patcher.New(patchReader, consumer)
					wtest.Must(t, err)

					targetPool := fspool.New(p.GetTargetContainer(), outDir)

					b, err := bowl.NewOverlayBowl(bowl.OverlayBowlParams{
						SourceContainer: p.GetSourceContainer(),
						TargetContainer: p.GetTargetContainer(),
						StageFolder:     stageDir,
						OutputFolder:    outDir,
					})
					wtest.Must(t, err)

					err = p.Resume(nil, targetPool, b)
					wtest.Must(t, err)

					wtest.Must(t, b.Commit())

					wtest.Must(t, assertValid(outDir, v2Sig))
					wtest.Must(t, pwr.AssertNoGhosts(outDir, v2Sig))
				}()
			}

			v1Heal := func() {
				log("Healing to (v1)")
				vctx := &pwr.ValidatorContext{
					HealPath: "archive," + v1zip,
					Consumer: consumer,
				}
				wtest.Must(t, vctx.Validate(context.Background(), outDir, v1Sig))
				wtest.Must(t, assertValid(outDir, v1Sig))
			}

			v1Heal()
			if scenario.corruptions != nil {
				log("Corrupting...")
				makeTestDir(t, outDir, *scenario.corruptions)
				v1Heal()
			}

			v2Heal := func() {
				log("Healing to (v2)")
				vctx := &pwr.ValidatorContext{
					HealPath: "archive," + v2zip,
					Consumer: consumer,
				}
				wtest.Must(t, vctx.Validate(context.Background(), outDir, v2Sig))
				wtest.Must(t, assertValid(outDir, v2Sig))
			}

			v2Heal()
			if scenario.corruptions != nil {
				log("Corrupting...")
				makeTestDir(t, outDir, *scenario.corruptions)
				v2Heal()
			}
		}()
	})
}
