package pwr_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools/fspool"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/wtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/itchio/wharf/compressors/cbrotli"
)

func Test_WritePatchSourceChanged(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resize func(size int64) int64
	}{
		{"grown", func(size int64) int64 { return size + wtest.BlockSize*3 + 5 }},
		{"shrunk", func(size int64) int64 { return size / 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			wtest.MakeTestDir(t, dir, wtest.TestDirSettings{
				Entries: []wtest.TestDirEntry{
					{Path: "a", Seed: 0x1},
					{Path: "logs/game.log", Seed: 0x2, Size: wtest.BlockSize*10 + 7},
					{Path: "z", Seed: 0x3},
				},
			})

			ctx := context.Background()
			container, err := tlc.WalkAny(ctx, dir, tlc.WalkOpts{})
			wtest.Must(t, err)

			logPath := filepath.Join(dir, "logs", "game.log")
			stat, err := os.Stat(logPath)
			wtest.Must(t, err)
			wtest.Must(t, os.Truncate(logPath, tc.resize(stat.Size())))
			newStat, err := os.Stat(logPath)
			wtest.Must(t, err)

			dctx := pwr.DiffContext{
				Compression:     &pwr.CompressionSettings{Algorithm: pwr.CompressionAlgorithm_BROTLI, Quality: 1},
				Consumer:        &state.Consumer{},
				SourceContainer: container,
				Pool:            fspool.New(container, dir),
				TargetContainer: &tlc.Container{},
			}

			err = dctx.WritePatch(ctx, io.Discard, io.Discard)

			var changed *pwr.SourceChangedError
			require.ErrorAs(t, err, &changed)
			assert.Equal(t, "logs/game.log", changed.Path)
			assert.Equal(t, stat.Size(), changed.ExpectedSize)
			assert.Equal(t, newStat.Size(), changed.ActualSize)
		})
	}
}
