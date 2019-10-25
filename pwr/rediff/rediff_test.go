package rediff_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/itchio/go-brotli/enc"
	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/savior"
	"github.com/itchio/savior/brotlisource"
	"github.com/itchio/savior/seeksource"
	"github.com/itchio/wharf/bsdiff"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/patcher"
	"github.com/itchio/wharf/pwr/rediff"
	"github.com/itchio/wharf/wtest"
)

type brotliCompressor struct{}

func (bc *brotliCompressor) Apply(writer io.Writer, quality int32) (io.Writer, error) {
	return enc.NewBrotliWriter(writer, &enc.BrotliWriterOptions{
		Quality: int(quality),
	}), nil
}

type brotliDecompressor struct{}

func (bc *brotliDecompressor) Apply(source savior.Source) (savior.Source, error) {
	return brotlisource.New(source), nil
}

func init() {
	pwr.RegisterCompressor(pwr.CompressionAlgorithm_BROTLI, &brotliCompressor{})
	pwr.RegisterDecompressor(pwr.CompressionAlgorithm_BROTLI, &brotliDecompressor{})
}

type rediffScenario struct {
	name       string
	v1         wtest.TestDirSettings
	v2         wtest.TestDirSettings
	partitions int
}

func Test_RediffOneSeq(t *testing.T) {
	runRediffScenario(t, rediffScenario{
		name: "rediff when one byte gets changed",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Data: []byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
				}},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Data: []byte{
					0x00,
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
				}},
			},
		},
	})
}

func Test_RediffOneInt(t *testing.T) {
	runRediffScenario(t, rediffScenario{
		name: "rediff when one byte gets changed",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Data: []byte{
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
				}},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "file", Data: []byte{
					0x00,
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
					0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19,
				}},
			},
		},
		partitions: 2,
	})
}

func Test_RediffWorse(t *testing.T) {
	runRediffScenario(t, rediffScenario{
		name: "rediff gets slightly worse",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*2 + 14},
				{Path: "file-1", Seed: 0x2},
				{Path: "dir2/file-2", Seed: 0x3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*3 + 14},
				{Path: "file-1", Seed: 0x2},
				{Path: "dir2/file-2", Seed: 0x33},
			},
		},
	})
}

func Test_RediffBetter(t *testing.T) {
	for _, partitions := range []int{0, 2, 4, 8} {
		runRediffScenario(t, rediffScenario{
			name: "rediff gets better!",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*5 + 14},
					{Path: "file-1", Seed: 0x2},
					{Path: "dir2/file-2", Seed: 0x3},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*5 + 14, Bsmods: []wtest.Bsmod{
						wtest.Bsmod{Interval: pwr.BlockSize/2 + 3, Delta: 0x4},
						wtest.Bsmod{Interval: pwr.BlockSize/3 + 7, Delta: 0x18},
					}},
					{Path: "file-1", Seed: 0x2},
					{Path: "dir2/file-2", Seed: 0x33},
				},
			},
			partitions: partitions,
		})
	}
}

func Test_RediffEdgeCases(t *testing.T) {
	for _, partitions := range []int{0, 2, 4, 8} {
		runRediffScenario(t, rediffScenario{
			name: "rediff gets better!",
			v1: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1, Size: 0},
					{Path: "file2", Seed: 0x2},
					{Path: "file3", Seed: 0x3, Size: 1},
					{Path: "file4", Seed: 0x5, Size: 8},
					{Path: "file5", Seed: 0x7, Size: 9},
				},
			},
			v2: wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "file1", Seed: 0x1},
					{Path: "file2", Seed: 0x2, Size: 0},
					{Path: "file3", Seed: 0x4, Size: 1},
					{Path: "file5", Seed: 0x6, Size: 8},
					{Path: "file6", Seed: 0x8, Size: 9},
				},
			},
			partitions: partitions,
		})
	}
}

func Test_RediffStillBetter(t *testing.T) {
	runRediffScenario(t, rediffScenario{
		name: "rediff gets better even though rsync wasn't that bad",
		v1: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize*5 + 14},
				{Path: "file-1", Seed: 0x2, Size: pwr.BlockSize * 4},
				{Path: "dir2/file-2", Seed: 0x3},
			},
		},
		v2: wtest.TestDirSettings{
			Entries: []wtest.TestDirEntry{
				{Path: "subdir/file-1", Seed: 0x1, Size: pwr.BlockSize * 6, Bsmods: []wtest.Bsmod{
					wtest.Bsmod{Interval: pwr.BlockSize/7 + 3, Delta: 0x4, Max: 4, Skip: 20},
					wtest.Bsmod{Interval: pwr.BlockSize/13 + 7, Delta: 0x18, Max: 6, Skip: 20},
				}},
				{Path: "file-1", Chunks: []wtest.TestDirChunk{
					wtest.TestDirChunk{Size: pwr.BlockSize*2 + 3, Seed: 0x99},
					wtest.TestDirChunk{Size: pwr.BlockSize*1 + 12, Seed: 0x2},
				}},
				{Path: "dir2/file-2", Seed: 0x33},
			},
		},
	})
}

func runRediffScenario(t *testing.T, scenario rediffScenario) {
	log := t.Logf

	mainDir, err := ioutil.TempDir("", "rediff")
	wtest.Must(t, err)
	wtest.Must(t, os.MkdirAll(mainDir, 0755))
	defer os.RemoveAll(mainDir)

	v1 := filepath.Join(mainDir, "v1")
	wtest.MakeTestDir(t, v1, scenario.v1)

	v2 := filepath.Join(mainDir, "v2")
	wtest.MakeTestDir(t, v2, scenario.v2)

	compression := &pwr.CompressionSettings{}
	compression.Algorithm = pwr.CompressionAlgorithm_BROTLI
	compression.Quality = 1

	sourceContainer, err := tlc.WalkAny(v2, &tlc.WalkOpts{})
	wtest.Must(t, err)

	consumer := &state.Consumer{
		OnMessage: func(level string, message string) {
			// log("[%s] %s", level, message)
		},
	}
	patchBuffer := new(bytes.Buffer)
	signatureBuffer := new(bytes.Buffer)

	func() {
		targetContainer, dErr := tlc.WalkAny(v1, &tlc.WalkOpts{})
		wtest.Must(t, dErr)

		targetPool := fspool.New(targetContainer, v1)
		targetSignature, dErr := pwr.ComputeSignature(context.Background(), targetContainer, targetPool, consumer)
		wtest.Must(t, dErr)

		log("Diffing %s -> %s",
			united.FormatBytes(targetContainer.Size),
			united.FormatBytes(sourceContainer.Size),
		)

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
	}()

	sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
	_, sigErr := sigReader.Resume(nil)
	wtest.Must(t, sigErr)

	signature, sErr := pwr.ReadSignature(context.Background(), sigReader)
	wtest.Must(t, sErr)

	v1Before := filepath.Join(mainDir, "v1Before")
	wtest.CpDir(t, v1, v1Before)

	v1After := filepath.Join(mainDir, "v1After")

	wtest.Must(t, os.RemoveAll(v1Before))
	wtest.CpDir(t, v1, v1Before)

	func() {
		err := patcher.PatchFresh(patcher.PatchFreshParams{
			PatchReader: seeksource.FromBytes(patchBuffer.Bytes()),

			TargetDir: v1Before,
			OutputDir: v1After,
		})
		wtest.Must(t, err)

		sigReader := seeksource.FromBytes(signatureBuffer.Bytes())
		_, sigErr := sigReader.Resume(nil)
		wtest.Must(t, sigErr)

		signature, sErr := pwr.ReadSignature(context.Background(), sigReader)
		wtest.Must(t, sErr)
		wtest.Must(t, pwr.AssertValid(v1After, signature))
		log("Original applies cleanly")
	}()

	func() {
		var stats bsdiff.DiffStats

		rc, err := rediff.NewContext(rediff.Params{
			Consumer:              consumer,
			Compression:           compression,
			SuffixSortConcurrency: 0,
			PatchReader:           seeksource.FromBytes(patchBuffer.Bytes()),
			Partitions:            scenario.partitions,

			BsdiffStats: &stats,
		})
		wtest.Must(t, err)

		log("Optimizing (%d partitions)...", rc.Partitions())

		optimizedPatchBuffer := new(bytes.Buffer)

		beforeOptimize := time.Now()
		oErr := rc.Optimize(rediff.OptimizeParams{
			TargetPool:  fspool.New(rc.GetTargetContainer(), v1),
			SourcePool:  fspool.New(rc.GetSourceContainer(), v2),
			PatchWriter: optimizedPatchBuffer,
		})
		wtest.Must(t, oErr)

		log("Optimized patch in %s (spent %s sorting, %s scanning)",
			time.Since(beforeOptimize),
			stats.TimeSpentSorting,
			stats.TimeSpentScanning,
		)

		before := patchBuffer.Len()
		after := optimizedPatchBuffer.Len()

		diff := (float64(after) - float64(before)) / float64(before) * 100
		if diff < 0 {
			log("Patch is %.2f%% smaller (%s < %s)", -diff, united.FormatBytes(int64(after)), united.FormatBytes(int64(before)))
		} else {
			log("Patch is %.2f%% larger (%s > %s)", diff, united.FormatBytes(int64(after)), united.FormatBytes(int64(before)))
		}

		wtest.Must(t, os.RemoveAll(v1Before))
		wtest.CpDir(t, v1, v1Before)

		func() {
			err := patcher.PatchFresh(patcher.PatchFreshParams{
				PatchReader: seeksource.FromBytes(optimizedPatchBuffer.Bytes()),

				TargetDir: v1Before,
				OutputDir: v1After,
			})
			wtest.Must(t, err)

			wtest.Must(t, pwr.AssertValid(v1After, signature))
			log("Optimized patch applies cleanly.")
		}()
	}()
}
