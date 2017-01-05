package pwr

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/Datadog/zstd"
	"github.com/alecthomas/assert"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
)

type zstdCompressor struct{}

func (zc *zstdCompressor) Apply(writer io.Writer, quality int32) (io.Writer, error) {
	return zstd.NewWriterLevel(writer, int(quality)), nil
}

type zstdDecompressor struct{}

func (zc *zstdDecompressor) Apply(reader io.Reader) (io.Reader, error) {
	return zstd.NewReader(reader), nil
}

func init() {
	RegisterCompressor(CompressionAlgorithm_ZSTD, &zstdCompressor{})
	RegisterDecompressor(CompressionAlgorithm_ZSTD, &zstdDecompressor{})
}

func Test_Rediff(t *testing.T) {
	runRediffScenario(t, patchScenario{
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

func runRediffScenario(t *testing.T, scenario patchScenario) {
	log := t.Logf

	mainDir, err := ioutil.TempDir("", "rediff")
	assert.NoError(t, err)
	assert.NoError(t, os.MkdirAll(mainDir, 0755))
	defer os.RemoveAll(mainDir)

	v1 := filepath.Join(mainDir, "v1")
	makeTestDir(t, v1, scenario.v1)

	v2 := filepath.Join(mainDir, "v2")
	makeTestDir(t, v2, scenario.v2)

	compression := &CompressionSettings{}
	compression.Algorithm = CompressionAlgorithm_ZSTD
	compression.Quality = 1

	sourceContainer, err := tlc.WalkAny(v2, nil)
	assert.NoError(t, err)

	consumer := &state.Consumer{
		OnMessage: func(level string, message string) {
			log("[%s] %s", level, message)
		},
	}
	patchBuffer := new(bytes.Buffer)
	signatureBuffer := new(bytes.Buffer)

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, nil)
		assert.NoError(t, dErr)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, dErr := ComputeSignature(targetContainer, targetPool, consumer)
		assert.NoError(t, dErr)

		pool := fspool.New(sourceContainer, v2)

		dctx := &DiffContext{
			Compression: compression,
			Consumer:    consumer,

			SourceContainer: sourceContainer,
			Pool:            pool,

			TargetContainer: targetContainer,
			TargetSignature: targetSignature,
		}

		assert.NoError(t, dctx.WritePatch(patchBuffer, signatureBuffer))
	}()

	v1Before := filepath.Join(mainDir, "v1Before")
	cpDir(t, v1, v1Before)

	v1After := filepath.Join(mainDir, "v1After")

	log("Applying to other directory, with separate check")
	assert.NoError(t, os.RemoveAll(v1Before))
	cpDir(t, v1, v1Before)

	func() {
		actx := &ApplyContext{
			TargetPath: v1Before,
			OutputPath: v1After,

			Consumer: consumer,
		}

		patchReader := bytes.NewReader(patchBuffer.Bytes())

		aErr := actx.ApplyPatch(patchReader)
		assert.NoError(t, aErr)
	}()

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, nil)
		assert.NoError(t, dErr)

		rc := &RediffContext{
			TargetPool: fspool.New(targetContainer, v1),
			SourcePool: fspool.New(sourceContainer, v2),

			Consumer:    consumer,
			Compression: compression,
		}

		patchReader := bytes.NewReader(patchBuffer.Bytes())

		log("Analyzing patch...")
		aErr := rc.AnalyzePatch(patchReader)
		assert.NoError(t, aErr)

		patchReader.Seek(0, os.SEEK_SET)
		patchReader = bytes.NewReader(patchBuffer.Bytes())

		optimizedPatchBuffer := new(bytes.Buffer)

		log("Optimizing patch...")
		oErr := rc.OptimizePatch(patchReader, optimizedPatchBuffer)
		assert.NoError(t, oErr)

		log("Before optimization: %d bytes", patchBuffer.Len())
		log("After  optimization: %d bytes", optimizedPatchBuffer.Len())
	}()
}
