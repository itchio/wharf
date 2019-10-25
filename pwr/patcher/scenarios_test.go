package patcher_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/screw"
	"github.com/itchio/wharf/archiver"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/pwr/rediff"
	"github.com/itchio/wharf/wtest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_Scenarios(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name: "becomes empty",
		// TODO: migrate to wtest.TestDirSettings etc.
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Seed: 0x1},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Seed: 0x1, Size: -1},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change one",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*11 + 14},
				{Path: "file-1", Seed: 0x2},
				{Path: "dir2/file-2", Seed: 0x3},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x22, Size: pwr.BlockSize*11 + 14},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*17 + 14},
				{Path: "file-1", Seed: 0x2},
				{Path: "dir2/file-2", Seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "one became short",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "short", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize},
				}},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "short", Chunks: []wtest.TestDirChunk{
						wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize - 17},
					}},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "short", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize},
					wtest.TestDirChunk{Seed: 0x222, Size: 17},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "early small wound",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "short", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 16},
				}},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "short", Chunks: []wtest.TestDirChunk{
						wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize},
						wtest.TestDirChunk{Seed: 0x222, Size: 1},
						wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize*15 - 1},
					}},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "short", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 16},
					wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize * 16},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change one in the middle",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize*12 + 1},
					wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize*12 + 3},
					wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize*12 + 4},
				}},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Chunks: []wtest.TestDirChunk{
						wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize*12 + 1},
						wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize*12 + 3},
						wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize * 12},
					}},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize*12 + 1},
					wtest.TestDirChunk{Seed: 0x444, Size: pwr.BlockSize*12 + 3},
					wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize*12 + 4},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "add one, remove one",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir2/file-1", Seed: 0x2},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir2/file-2", Seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "rename one",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir2/subdir/file-1", Seed: 0x2, Size: pwr.BlockSize*12 + 13},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir3/subdir/subdir/file-2", Seed: 0x2, Size: pwr.BlockSize*12 + 13},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "delete folder, one generated",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir2/subdir/file-1", Seed: 0x2, Size: pwr.BlockSize*12 + 13},
			},
		},
		intermediate: &wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir2/subdir/file-1-generated", Seed: 0x999, Size: pwr.BlockSize * 3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/subdir/file-1", Seed: 0x1},
				{Path: "dir3/subdir/subdir/file-2", Seed: 0x289, Size: pwr.BlockSize*3 + 12},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "move 4 files",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "old/file-1", Seed: 0x111},
				{Path: "old/subdir/file-1", Seed: 0x222},
				{Path: "old/subdir/file-2", Seed: 0x333},
				{Path: "old/subdir/subdir/file-4", Seed: 0x444},
			},
		},
		intermediate: &wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "old/subdir/file-1-generated", Seed: 0x999, Size: pwr.BlockSize * 3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "new/file-1", Seed: 0x111},
				{Path: "new/subdir/file-1", Seed: 0x222},
				{Path: "new/subdir/file-2", Seed: 0x333},
				{Path: "new/subdir/subdir/file-4", Seed: 0x444},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "move 4 files into a subdirectory",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "old/file-1", Seed: 0x1},
				{Path: "old/subdir/file-1", Seed: 0x2},
				{Path: "old/subdir/file-2", Seed: 0x3},
				{Path: "old/subdir/subdir/file-4", Seed: 0x4},
			},
		},
		intermediate: &wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "old/subdir/file-1-generated", Seed: 0x999, Size: pwr.BlockSize * 3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "old/new/file-1", Seed: 0x1},
				{Path: "old/new/subdir/file-1", Seed: 0x2},
				{Path: "old/new/subdir/file-2", Seed: 0x3},
				{Path: "old/new/subdir/subdir/file-4", Seed: 0x4},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "one file is duplicated twice",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/file-1", Seed: 0x1},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/file-1", Seed: 0x1},
				{Path: "dir2/file-1", Seed: 0x1},
				{Path: "dir2/file-1bis", Seed: 0x1},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "one file is renamed + duplicated twice",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir1/file-1", Seed: 0x1},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "dir2/file-1", Seed: 0x1},
				{Path: "dir3/file-1", Seed: 0x1},
				{Path: "dir3/file-1bis", Seed: 0x1},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large unchanged",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x99, Size: pwr.BlockSize*largeAmount + 17},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x99, Size: pwr.BlockSize*largeAmount + 17},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap + duplicate (option A)",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x99, Size: pwr.BlockSize*largeAmount + 17},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "four large, two swap + duplicate (option B)",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x33, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x44, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
		corruptions: &testCorruption{
			files: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x99, Size: pwr.BlockSize*largeAmount + 17},
				},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x22, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-2", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-3", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
				{Path: "subdir/file-4", Seed: 0x11, Size: pwr.BlockSize*largeAmount + 17},
			},
		},
	})

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are added by patch",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file", Seed: 0x1},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file", Seed: 0x1},
					{Path: "dir1/link", Dest: "file"},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are changed by patch",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file1", Seed: 0x1},
					{Path: "dir1/file2", Seed: 0x2},
					{Path: "dir1/link", Dest: "file1"},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file1", Seed: 0x1},
					{Path: "dir1/file2", Seed: 0x2},
					{Path: "dir1/link", Dest: "file2"},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlinks are removed by patch",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file", Seed: 0x1},
					{Path: "dir1/link", Dest: "file"},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "dir1/file", Seed: 0x1},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes normal file (itchio/itch#2315)",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test2.txt", Seed: 0x1},
					{Path: "test.txt", Dest: "test2.txt"},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test2.txt", Seed: 0x1},
					{Path: "test.txt", Seed: 0x2},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes directory",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test2.txt", Seed: 0x1},
					{Path: "test.txt", Dest: "test2.txt"},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test2.txt", Seed: 0x1},
					{Path: "test.txt/woop", Seed: 0x2},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "symlink becomes dangling",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test2.txt", Seed: 0x1},
					{Path: "test.txt", Dest: "test2.txt"},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "test.txt", Dest: "test2.txt"},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "multi-level symlink 1",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1},
					{Path: "file2", Seed: 0x2},
					{Path: "file3", Seed: 0x3},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1},
					{Path: "file2", Seed: 0x2},
					{Path: "file3", Seed: 0x3},
					{Path: "aaa.txt", Dest: "aa.txt"},
					{Path: "aa.txt", Dest: "a.txt"},
					{Path: "a.txt", Seed: 0x4},
				},
			},
		})
	}

	if testSymlinks {
		runPatchingScenario(t, patchScenario{
			name: "multi-level symlink 2",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1},
					{Path: "file2", Seed: 0x2},
					{Path: "file3", Seed: 0x3},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1},
					{Path: "file2", Seed: 0x2},
					{Path: "file3", Seed: 0x3},
					{Path: "bbb.txt", Seed: 0x4},
					{Path: "bb.txt", Dest: "bbb.txt"},
					{Path: "b.txt", Dest: "bb.txt"},
				},
			},
		})
	}

	runPatchingScenario(t, patchScenario{
		name: "change case",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "apricot", Seed: 0x1},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "APRICOT", Seed: 0x1},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change parent case",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "base/apricot", Seed: 0x1},
				{Path: "base/apple", Seed: 0x2},
				{Path: "base/orange", Seed: 0x3},
			},
		},
		corruptions: &testCorruption{
			before: func(t *testing.T, dir string) {
				wtest.Must(t, screw.Rename(
					filepath.Join(dir, "base"),
					filepath.Join(dir, "BASE"),
				))
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "BASE/apricot", Seed: 0x1},
				{Path: "BASE/apple", Seed: 0x2},
				{Path: "BASE/orange", Seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "patch with parent case changed",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "base/apricot", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 8},
					wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize * 8},
				}},
				{Path: "base/apple", Seed: 0x2},
				{Path: "base/orange", Seed: 0x3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "BASE/apricot", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 8},
					wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize * 1},
					wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize * 7},
				}},
				{Path: "BASE/apple", Seed: 0x2},
				{Path: "BASE/orange", Seed: 0x3},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "rename with parent case changed",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "base/apple", Seed: 0x2},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "BASE/orange", Seed: 0x2},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change case and patch",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "apricot", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 8},
					wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize * 8},
				}},
			},
		},
		corruptions: &testCorruption{
			before: func(t *testing.T, dir string) {
				wtest.Must(t, screw.Rename(
					filepath.Join(dir, "apricot"),
					filepath.Join(dir, "APRICOT"),
				))
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "APRICOT", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Seed: 0x111, Size: pwr.BlockSize * 8},
					wtest.TestDirChunk{Seed: 0x333, Size: pwr.BlockSize * 1},
					wtest.TestDirChunk{Seed: 0x222, Size: pwr.BlockSize * 7},
				}},
			},
		},
	})

	runPatchingScenario(t, patchScenario{
		name: "change case of unrelated directory",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "DataSomething.dll", Seed: 0x1},
				{Path: "Data/Hello.txt", Seed: 0x2},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "DataSomething.dll", Seed: 0x1},
				{Path: "data/Hello.txt", Seed: 0x2},
			},
		},
	})
}

type ScenarioDirection int

const (
	ScenarioForward ScenarioDirection = 1
	ScenarioBack    ScenarioDirection = 2
)

func runPatchingScenario(t *testing.T, scenario patchScenario) {
	forwardScenario := *&scenario
	forwardScenario.name += "/forward"
	runSinglePatchingScenario(t, forwardScenario, ScenarioForward)

	backScenario := *&scenario
	backScenario.name += "/back"
	backScenario.v1, backScenario.v2 = backScenario.v2, backScenario.v1
	runSinglePatchingScenario(t, backScenario, ScenarioBack)
}

func runSinglePatchingScenario(t *testing.T, scenario patchScenario, direction ScenarioDirection) {
	enableCorruptions := scenario.corruptions != nil && direction == ScenarioForward

	t.Run(scenario.name, func(t *testing.T) {
		log := t.Logf

		mainDir, err := ioutil.TempDir("", "patch-cycle")
		wtest.Must(t, err)
		defer screw.RemoveAll(mainDir)

		v1 := filepath.Join(mainDir, "v1")
		wtest.MakeTestDir(t, v1, scenario.v1)

		v2 := filepath.Join(mainDir, "v2")
		wtest.MakeTestDir(t, v2, scenario.v2)

		v1zip := filepath.Join(mainDir, "v1.zip")
		v2zip := filepath.Join(mainDir, "v2.zip")

		func() {
			fw, err := screw.Create(v1zip)
			wtest.Must(t, err)

			_, err = archiver.CompressZip(fw, v1, nil)
			wtest.Must(t, err)
		}()

		func() {
			fw, err := screw.Create(v2zip)
			wtest.Must(t, err)

			_, err = archiver.CompressZip(fw, v2, nil)
			wtest.Must(t, err)
		}()

		consumer := &state.Consumer{
			OnProgressLabel: func(label string) {
				t.Logf("> %s", label)
			},
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
			rc, err := rediff.NewContext(rediff.Params{
				PatchReader: seeksource.FromBytes(patchBuffer.Bytes()),

				Consumer:              consumer,
				Compression:           compression,
				SuffixSortConcurrency: 0,
				Partitions:            2,
			})
			wtest.Must(t, err)

			wtest.Must(t, rc.Optimize(rediff.OptimizeParams{
				TargetPool:  fspool.New(rc.GetTargetContainer(), v1),
				SourcePool:  fspool.New(rc.GetSourceContainer(), v2),
				PatchWriter: optimizedPatchBuffer,
			}))
		}()

		log("    Naive patch: %s", united.FormatBytes(int64(patchBuffer.Len())))
		log("Optimized patch: %s", united.FormatBytes(int64(optimizedPatchBuffer.Len())))

		func() {
			outDir := filepath.Join(mainDir, "out")
			wtest.WipeAndMkdir(t, outDir)

			stageDir := filepath.Join(mainDir, "stage")
			wtest.WipeAndMkdir(t, stageDir)

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
					wtest.WipeAndMkdir(t, outDir)

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
					defer b.Close()

					err = p.Resume(nil, targetPool, b)
					wtest.Must(t, err)

					wtest.Must(t, b.Commit())

					wtest.Must(t, assertValid(outDir, v2Sig))
					wtest.Must(t, pwr.AssertNoGhosts(outDir, v2Sig))
				}()

				applyInPlace := func(beforePatch func()) error {
					wtest.WipeAndCpDir(t, v1, outDir)
					beforePatch()

					patchReader := seeksource.FromBytes(patch.Buffer.Bytes())
					_, err = patchReader.Resume(nil)
					if err != nil {
						return errors.WithStack(err)
					}

					p, err := patcher.New(patchReader, consumer)
					if err != nil {
						return errors.WithStack(err)
					}

					targetPool := fspool.New(p.GetTargetContainer(), outDir)

					b, err := bowl.NewOverlayBowl(bowl.OverlayBowlParams{
						SourceContainer: p.GetSourceContainer(),
						TargetContainer: p.GetTargetContainer(),
						StageFolder:     stageDir,
						OutputFolder:    outDir,

						Consumer: consumer,
					})
					if err != nil {
						return errors.WithStack(err)
					}
					defer b.Close()

					err = p.Resume(nil, targetPool, b)
					if err != nil {
						return errors.WithStack(err)
					}

					err = b.Commit()
					if err != nil {
						return err
					}

					err = assertValid(outDir, v2Sig)
					if err != nil {
						return errors.WithStack(err)
					}
					return nil
				}

				if enableCorruptions {
					func() {
						log("Applying %s in-place (v1 + corruptions) -> (v2)", patch.Name)
						err := applyInPlace(func() {
							applyCorruptions(t, outDir, *scenario.corruptions)
						})
						if err != nil {
							log("As expected, got an error: %v", err)
						}
						if patch.Name == "naive" {
							// sometimes the optimized patches work anyway?
							assert.Error(t, err)
						}
					}()
				}

				if scenario.intermediate != nil {
					func() {
						log("Applying %s in-place (v1 + intermediate) -> (v2)", patch.Name)
						err := applyInPlace(func() {
							wtest.MakeTestDir(t, outDir, *scenario.intermediate)
						})
						wtest.Must(t, err)
					}()
				}

				func() {
					log("Applying %s in-place (v1) -> (v2)", patch.Name)
					wtest.Must(t, applyInPlace(func() {}))
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

			if enableCorruptions {
				wtest.WipeAndCpDir(t, v1, outDir)
				log("Corrupting...")
				applyCorruptions(t, outDir, *scenario.corruptions)
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
		}()
	})
}
