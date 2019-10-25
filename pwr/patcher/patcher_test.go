package patcher_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/united"
	"github.com/itchio/screw"
	"github.com/itchio/wharf/wsync"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/pwr/rediff"
	"github.com/itchio/wharf/wtest"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior/seeksource"

	_ "github.com/itchio/wharf/compressors/cbrotli"
	_ "github.com/itchio/wharf/decompressors/cbrotli"
)

func Test_Naive(t *testing.T) {
	dir, err := ioutil.TempDir("", "patcher-noop")
	wtest.Must(t, err)
	defer screw.RemoveAll(dir)

	v1 := filepath.Join(dir, "v1")
	wtest.MakeTestDir(t, v1, wtest.TestDirSettings{
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*220 + 14},
			{Path: "file-1", Seed: 0x2},
			{Path: "dir2/file-2", Seed: 0x3},
			{Path: "dir3/gone", Seed: 0x4},
		},
	})

	v2 := filepath.Join(dir, "v2")
	wtest.MakeTestDir(t, v2, wtest.TestDirSettings{
		Entries: []wtest.TestDirEntry{
			{Path: "subdir/file-1", Seed: 0x1, Size: wtest.BlockSize*260 + 14, Bsmods: []wtest.Bsmod{
				{Interval: wtest.BlockSize/2 + 3, Delta: 0x4},
				{Interval: wtest.BlockSize/3 + 7, Delta: 0x18},
			}, Swaperoos: []wtest.Swaperoo{
				{OldStart: 0, NewStart: wtest.BlockSize * 210, Size: wtest.BlockSize * 10},
				{OldStart: 40, NewStart: wtest.BlockSize*10 + 8, Size: wtest.BlockSize * 40},
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
		sourceHashes, err = pwr.ComputeSignature(context.Background(), sourceContainer, fspool.New(sourceContainer, v2), consumer)
		wtest.Must(t, err)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, err := pwr.ComputeSignature(context.Background(), targetContainer, targetPool, consumer)
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

		wtest.Must(t, dctx.WritePatch(context.Background(), patchBuffer, ioutil.Discard))

		// Rediff!
		t.Logf("Rediffing...")
		rc, err := rediff.NewContext(rediff.Params{
			Consumer:    consumer,
			PatchReader: seeksource.FromBytes(patchBuffer.Bytes()),
		})
		wtest.Must(t, err)

		wtest.Must(t, rc.Optimize(rediff.OptimizeParams{
			TargetPool:  targetPool,
			SourcePool:  pool,
			PatchWriter: optimizedPatchBuffer,
		}))
	}

	// Patch!
	tryPatchNoSaves := func(t *testing.T, patchBytes []byte) {
		consumer := &state.Consumer{
			OnMessage: func(level string, message string) {
				t.Logf("[%s] %s", level, message)
			},
		}

		out := filepath.Join(dir, "out")
		defer screw.RemoveAll(out)

		patchReader := seeksource.FromBytes(patchBytes)

		p, err := patcher.New(patchReader, consumer)
		wtest.Must(t, err)

		targetPool := fspool.New(p.GetTargetContainer(), v1)

		b, err := bowl.NewFreshBowl(bowl.FreshBowlParams{
			SourceContainer: p.GetSourceContainer(),
			TargetContainer: p.GetTargetContainer(),
			TargetPool:      targetPool,
			OutputFolder:    out,
		})
		wtest.Must(t, err)

		err = p.Resume(nil, targetPool, b)
		wtest.Must(t, err)

		// Validate!
		sigInfo := &pwr.SignatureInfo{
			Container: p.GetSourceContainer(),
			Hashes:    sourceHashes,
		}
		wtest.Must(t, pwr.AssertValid(out, sigInfo))
		wtest.Must(t, pwr.AssertNoGhosts(out, sigInfo))

		t.Logf("Patch applies cleanly!")
	}

	tryPatchWithSaves := func(t *testing.T, patchBytes []byte) {
		consumer := &state.Consumer{
			OnMessage: func(level string, message string) {
				t.Logf("[%s] %s", level, message)
			},
		}

		out := filepath.Join(dir, "out")
		defer screw.RemoveAll(out)

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

		b, err := bowl.NewFreshBowl(bowl.FreshBowlParams{
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
			if errors.Cause(err) == patcher.ErrStop {
				t.Logf("Patcher returned ErrStop")

				if checkpoint == nil {
					wtest.Must(t, errors.New("patcher stopped but nil checkpoint"))
				}
				numCheckpoints++

				checkpointBuf := new(bytes.Buffer)
				enc := gob.NewEncoder(checkpointBuf)
				wtest.Must(t, enc.Encode(checkpoint))

				t.Logf("Got %s checkpoint @ %.2f%% of the patch", united.FormatBytes(int64(checkpointBuf.Len())), p.Progress()*100.0)

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
		t.Run(fmt.Sprintf("%s-no-saves", kind), func(t *testing.T) {
			t.Logf("Applying %s %s patch (%d bytes), no saves", united.FormatBytes(int64(len(patchBytes))), kind, len(patchBytes))
			tryPatchNoSaves(t, patchBytes)
		})

		t.Run(fmt.Sprintf("%s-with-saves", kind), func(t *testing.T) {
			t.Logf("Applying %s %s patch (%d bytes) with saves", united.FormatBytes(int64(len(patchBytes))), kind, len(patchBytes))
			tryPatchWithSaves(t, patchBytes)
		})
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
