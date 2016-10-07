package pwr

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
)

// This is more of an integration test, it hits a lot of statements
func Test_PatchCycle(t *testing.T) {
	mainDir, err := ioutil.TempDir("", "patch-cycle")
	assert.Nil(t, err)
	assert.Nil(t, os.MkdirAll(mainDir, 0755))
	defer os.RemoveAll(mainDir)

	v1 := filepath.Join(mainDir, "v1")
	makeTestDir(t, v1, testDirSettings{
		fakeDataSize: 8*BlockSize + 128,
		seed:         0xf237,
	})

	v2 := filepath.Join(mainDir, "v2")
	makeTestDir(t, v2, testDirSettings{
		fakeDataSize: 8*BlockSize + 64,
		seed:         0xf239,
	})

	// mess with v2 so there's something to re-use in the patch
	cpFile(t, path.Join(v1, "file-2"), path.Join(v2, "file-2"))

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

	t.Logf("Applying to other directory, with separate check")

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
		assert.Equal(t, len(sourceContainer.Files), actx.Stats.TouchedFiles)
		assert.Equal(t, 0, actx.Stats.NoopFiles)

		signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
		assert.Nil(t, sErr)

		assert.Nil(t, AssertValid(v1After, signature))
	}()

	t.Logf("Applying in-place")

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

		assert.Equal(t, 0, actx.Stats.DeletedFiles)
		assert.Equal(t, len(sourceContainer.Files)-1, actx.Stats.TouchedFiles)
		assert.Equal(t, 1, actx.Stats.NoopFiles)

		signature, sErr := ReadSignature(bytes.NewReader(signatureBuffer.Bytes()))
		assert.Nil(t, sErr)

		assert.Nil(t, AssertValid(v1Before, signature))
	}()

	t.Logf("Refusing to vet")

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
		assert.Equal(t, 0, actx.Stats.TouchedFiles)
		assert.Equal(t, 0, actx.Stats.NoopFiles)
	}()
}
