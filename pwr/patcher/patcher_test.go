package patcher_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/wharf/pwr/bowl"

	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"

	"github.com/itchio/wharf/wtest"

	_ "github.com/itchio/wharf/compressors/cbrotli"
	_ "github.com/itchio/wharf/decompressors/cbrotli"
)

func Test_Naive(t *testing.T) {
	dir, err := ioutil.TempDir("", "patcher-noop")
	wtest.Must(t, err)
	defer os.RemoveAll(dir)

	v1 := filepath.Join(dir, "v1")
	wtest.MakeTestDir(t, v1, wtest.TestDirSettings{
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*11 + 14},
			{Path: "file-1", Seed: 0x2},
			{Path: "dir2/file-2", Seed: 0x3},
		},
	})

	v2 := filepath.Join(dir, "v2")
	wtest.MakeTestDir(t, v2, wtest.TestDirSettings{
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*17 + 14},
			{Path: "file-1", Seed: 0x2},
			{Path: "dir2/file-2", Seed: 0x3},
		},
	})

	patchBuffer := new(bytes.Buffer)
	consumer := &state.Consumer{
		OnMessage: func(level string, message string) {
			t.Logf("[%s] %s", level, message)
		},
	}

	// Diff!
	{
		compression := &pwr.CompressionSettings{}
		compression.Algorithm = pwr.CompressionAlgorithm_NONE
		compression.Quality = 9

		targetContainer, err := tlc.WalkAny(v1, &tlc.WalkOpts{})
		wtest.Must(t, err)

		sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
		wtest.Must(t, err)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, err := pwr.ComputeSignature(targetContainer, targetPool, consumer)
		wtest.Must(t, err)

		pool := fspool.New(sourceContainer, v2)

		dctx := pwr.DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		wtest.Must(t, dctx.WritePatch(patchBuffer, ioutil.Discard))
	}

	// Patch!
	{
		out := filepath.Join(dir, "out")

		patchReader := seeksource.FromBytes(patchBuffer.Bytes())

		p, err := patcher.New(patchReader, consumer)
		wtest.Must(t, err)

		targetPool := fspool.New(p.GetTargetContainer(), v1)

		b, err := bowl.NewFreshBowl(&bowl.FreshBowlParams{
			SourceContainer: p.GetSourceContainer(),
			TargetContainer: p.GetTargetContainer(),
			TargetPool:      targetPool,
			OutputFolder:    out,
		})
		wtest.Must(t, err)

		err = p.Resume(nil, targetPool, b)
		wtest.Must(t, err)
	}
}
