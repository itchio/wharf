package patcher_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/archiver"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/wtest"
)

func Test_ChangeOne(t *testing.T) {
	runPatchingScenario(t, patchScenario{
		name:         "change one",
		touchedFiles: 1,
		deletedFiles: 0,
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
		healedBytes: pwr.BlockSize*17 + 14,
		extraTests:  true,
		testVet:     true,
	})
}

func runPatchingScenario(t *testing.T, scenario patchScenario) {
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

	compression := &pwr.CompressionSettings{}
	compression.Algorithm = pwr.CompressionAlgorithm_NONE

	sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
	wtest.Must(t, err)

	patchBuffer := new(bytes.Buffer)
	signatureBuffer := new(bytes.Buffer)
	var v1Sig *pwr.SignatureInfo
	var v2Sig *pwr.SignatureInfo

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, &tlc.WalkOpts{})
		wtest.Must(t, dErr)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, dErr := pwr.ComputeSignature(context.Background(), targetContainer, targetPool, consumer)
		wtest.Must(t, dErr)

		pool := fspool.New(sourceContainer, v2)

		dctx := &pwr.DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		wtest.Must(t, dctx.WritePatch(context.Background(), patchBuffer, signatureBuffer))

		sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
		_, err = sigReader.Resume(nil)
		wtest.Must(t, err)

		v1Sig, err = pwr.ReadSignature(context.Background(), sigReader)
		wtest.Must(t, err)
	}()

	log("Applying to other directory, with separate check")
	func() {
		consumer := &state.Consumer{
			OnMessage: func(level string, message string) {
				t.Logf("[%s] %s", level, message)
			},
		}

		outDir := filepath.Join(mainDir, "out")
		wtest.Must(t, os.MkdirAll(outDir, 0o755))
		defer os.RemoveAll(outDir)

		patchReader := seeksource.FromBytes(patchBuffer.Bytes())
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

		wtest.Must(t, pwr.AssertValid(outDir, v1Sig))
		wtest.Must(t, pwr.AssertNoGhosts(outDir, v1Sig))
		log("Applies cleanly + no ghosts!")
	}()

	log("Applying in-place")
	func() {
		outDir := filepath.Join("out")
		wtest.Must(t, os.MkdirAll(outDir, 0o755))
		cpDir(t, v1, outDir)

		stageDir := filepath.Join("stage")
		wtest.Must(t, os.MkdirAll(stageDir, 0o755))

		patchReader := seeksource.FromBytes(patchBuffer.Bytes())
		_, err = patchReader.Resume(nil)
		wtest.Must(t, err)

		p, err := patcher.New(patchReader, consumer)
		wtest.Must(t, err)

		targetPool := fspool.New(p.GetTargetContainer(), v1)

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

		wtest.Must(t, pwr.AssertValid(outDir, v1Sig))
		wtest.Must(t, pwr.AssertNoGhosts(outDir, v1Sig))
		log("Applies cleanly + no ghosts!")

		log("Healing to previous version...")
		vctx := &pwr.ValidatorContext{
			HealPath: "archive," + v1zip,
			Consumer: consumer,
		}
		wtest.Must(t, vctx.Validate(context.Background(), outDir, v1Sig))

		log("Checking we're back to v1 after heal")
		vctx = &pwr.ValidatorContext{
			FailFast: true,
			Consumer: consumer,
		}
	}()
}
