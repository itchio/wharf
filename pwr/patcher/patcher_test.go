package patcher_test

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/wsync"
	"github.com/stretchr/testify/assert"

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
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*60 + 14},
			{Path: "file-1", Seed: 0x2},
			{Path: "dir2/file-2", Seed: 0x3},
		},
	})

	v2 := filepath.Join(dir, "v2")
	wtest.MakeTestDir(t, v2, wtest.TestDirSettings{
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*80 + 14, Bsmods: []wtest.Bsmod{
				{Interval: wtest.BlockSize/2 + 3, Delta: 0x4},
				{Interval: wtest.BlockSize/3 + 7, Delta: 0x18},
			}},
			{Path: "file-1", Seed: 0x2},
			{Path: "dir2/file-2", Seed: 0x3},
		},
	})

	patchBuffer := new(bytes.Buffer)
	optimizedPatchBuffer := new(bytes.Buffer)
	var sourceHashes []wsync.BlockHash
	consumer := &state.Consumer{
		OnMessage: func(level string, message string) {
			t.Logf("[%s] %s", level, message)
		},
	}

	{
		compression := &pwr.CompressionSettings{}
		compression.Algorithm = pwr.CompressionAlgorithm_BROTLI
		compression.Quality = 1

		targetContainer, err := tlc.WalkAny(v1, &tlc.WalkOpts{})
		wtest.Must(t, err)

		sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
		wtest.Must(t, err)

		// Sign!
		t.Logf("Signing %s", sourceContainer.Stats())
		sourceHashes, err = pwr.ComputeSignature(sourceContainer, fspool.New(sourceContainer, v2), consumer)
		wtest.Must(t, err)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, err := pwr.ComputeSignature(targetContainer, targetPool, consumer)
		wtest.Must(t, err)

		pool := fspool.New(sourceContainer, v2)

		// Diff!
		t.Logf("Diffing (%s)...", compression)
		dctx := pwr.DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		wtest.Must(t, dctx.WritePatch(patchBuffer, ioutil.Discard))

		// Rediff!
		t.Logf("Rediffing...")
		rc := pwr.RediffContext{
			Consumer: consumer,

			TargetContainer: targetContainer,
			TargetPool:      targetPool,

			SourceContainer: sourceContainer,
			SourcePool:      pool,
		}

		patchReader := seeksource.FromBytes(patchBuffer.Bytes())
		_, err = patchReader.Resume(nil)
		wtest.Must(t, err)

		wtest.Must(t, rc.AnalyzePatch(patchReader))

		_, err = patchReader.Resume(nil)
		wtest.Must(t, err)

		wtest.Must(t, rc.OptimizePatch(patchReader, optimizedPatchBuffer))
	}

	// Patch!
	tryPatchNoSaves := func(patchBytes []byte) {
		t.Logf("Patching without saves")

		out := filepath.Join(dir, "out")
		defer os.RemoveAll(out)

		patchReader := seeksource.FromBytes(patchBytes)

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

		// Validate!
		wtest.Must(t, pwr.AssertValid(out, &pwr.SignatureInfo{
			Container: p.GetSourceContainer(),
			Hashes:    sourceHashes,
		}))

		t.Logf("Patch applies cleanly!")
	}

	tryPatchWithSaves := func(patchBytes []byte) {
		t.Logf("Patching with saves!")

		out := filepath.Join(dir, "out")
		defer os.RemoveAll(out)

		patchReader := seeksource.FromBytes(patchBytes)

		p, err := patcher.New(patchReader, consumer)
		wtest.Must(t, err)

		var checkpoint *patcher.Checkpoint
		p.SetSaveConsumer(&patcherSaveConsumer{
			shouldSave: func() bool {
				return true
			},
			save: func(c *patcher.Checkpoint) (patcher.AfterSaveAction, error) {
				checkpoint = c
				return patcher.AfterSaveStop, nil
			},
		})

		targetPool := fspool.New(p.GetTargetContainer(), v1)

		b, err := bowl.NewFreshBowl(&bowl.FreshBowlParams{
			SourceContainer: p.GetSourceContainer(),
			TargetContainer: p.GetTargetContainer(),
			TargetPool:      targetPool,
			OutputFolder:    out,
		})
		wtest.Must(t, err)

		numCheckpoints := 0
		for {
			c := checkpoint
			checkpoint = nil
			t.Logf("Resuming patcher - has checkpoint: %v", c != nil)
			err = p.Resume(c, targetPool, b)
			if errors.Is(err, patcher.ErrStop) {
				t.Logf("Patcher returned ErrStop")

				if checkpoint == nil {
					wtest.Must(t, errors.New("patcher stopped but nil checkpoint"))
				}
				numCheckpoints++

				checkpointBuf := new(bytes.Buffer)
				enc := gob.NewEncoder(checkpointBuf)
				wtest.Must(t, enc.Encode(checkpoint))

				t.Logf("Got %s checkpoint @ %.2f%% of the patch", humanize.IBytes(uint64(checkpointBuf.Len())), p.Progress()*100.0)

				checkpoint = &patcher.Checkpoint{}
				dec := gob.NewDecoder(bytes.NewReader(checkpointBuf.Bytes()))
				wtest.Must(t, dec.Decode(checkpoint))

				continue
			}

			wtest.Must(t, err)
			break
		}

		// Validate!
		wtest.Must(t, pwr.AssertValid(out, &pwr.SignatureInfo{
			Container: p.GetSourceContainer(),
			Hashes:    sourceHashes,
		}))

		t.Logf("Patch applies cleanly!")

		t.Logf("Had %d checkpoints total", numCheckpoints)
		assert.True(t, numCheckpoints > 0, "had at least one checkpoint")
	}

	tryPatch := func(kind string, patchBytes []byte) {
		t.Logf("Applying %s %s patch (%d bytes)", humanize.IBytes(uint64(len(patchBytes))), kind, len(patchBytes))
		tryPatchNoSaves(patchBytes)
		tryPatchWithSaves(patchBytes)
	}

	tryPatch("simple", patchBuffer.Bytes())
	tryPatch("optimized", optimizedPatchBuffer.Bytes())
}

//

type patcherSaveConsumer struct {
	shouldSave func() bool
	save       func(checkpoint *patcher.Checkpoint) (patcher.AfterSaveAction, error)
}

var _ patcher.SaveConsumer = (*patcherSaveConsumer)(nil)

func (psc *patcherSaveConsumer) ShouldSave() bool {
	return psc.shouldSave()
}

func (psc *patcherSaveConsumer) Save(checkpoint *patcher.Checkpoint) (patcher.AfterSaveAction, error) {
	return psc.save(checkpoint)
}
