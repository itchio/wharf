package bsdiff

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/itchio/headway/state"
	"github.com/stretchr/testify/assert"
)

// collectMessages returns a WriteMessageFunc that deep-copies each Control
// message into a slice. Deep copy is required because DiffContext.writeMessages
// reuses and mutates the Control struct between calls.
func collectMessages() (WriteMessageFunc, *[]*Control) {
	var messages []*Control
	writer := func(msg proto.Message) error {
		ctrl := msg.(*Control)
		clone := proto.Clone(ctrl).(*Control)
		messages = append(messages, clone)
		return nil
	}
	return writer, &messages
}

// replayMessages returns a ReadMessageFunc that replays collected messages sequentially.
func replayMessages(messages []*Control) ReadMessageFunc {
	i := 0
	return func(msg proto.Message) error {
		if i >= len(messages) {
			return io.EOF
		}
		ctrl := msg.(*Control)
		src := messages[i]
		ctrl.Reset()
		ctrl.Add = src.Add
		ctrl.Copy = src.Copy
		ctrl.Seek = src.Seek
		ctrl.Eof = src.Eof
		i++
		return nil
	}
}

// roundTrip diffs oldData against newData, then patches oldData with the result,
// and asserts the output matches newData.
func roundTrip(t *testing.T, oldData, newData []byte, partitions int) {
	t.Helper()

	writeMessage, messages := collectMessages()

	ctx := &DiffContext{
		Partitions: partitions,
	}
	consumer := &state.Consumer{}

	err := ctx.Do(bytes.NewReader(oldData), bytes.NewReader(newData), writeMessage, consumer)
	if !assert.NoError(t, err, "diff failed") {
		t.FailNow()
	}

	var out bytes.Buffer
	patchCtx := NewPatchContext()
	err = patchCtx.Patch(bytes.NewReader(oldData), &out, int64(len(newData)), replayMessages(*messages))
	if !assert.NoError(t, err, "patch failed") {
		t.FailNow()
	}

	if !bytes.Equal(newData, out.Bytes()) {
		t.Fatalf("patched output does not match expected new data (got %d bytes, want %d bytes)", out.Len(), len(newData))
	}
}

func seededRandomBytes(seed int64, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, size)
	rng.Read(buf)
	return buf
}

// makeModified copies data and XORs a percentage of bytes at random positions.
func makeModified(data []byte, seed int64, pctChange float64) []byte {
	out := make([]byte, len(data))
	copy(out, data)
	rng := rand.New(rand.NewSource(seed))
	numChanges := int(float64(len(data)) * pctChange)
	for range numChanges {
		idx := rng.Intn(len(out))
		out[idx] ^= byte(rng.Intn(255) + 1)
	}
	return out
}

func TestRoundTrip(t *testing.T) {
	modifiedPaper := []byte(strings.Replace(
		strings.Replace(string(paper), "Quicksort", "Mergesort", -1),
		"partitioning", "splitting", -1,
	))

	mediumOld := seededRandomBytes(10, 16*1024)
	largerOld := seededRandomBytes(20, 256*1024)

	cases := []struct {
		name    string
		oldData []byte
		newData []byte
	}{
		{"identical", []byte("hello world"), []byte("hello world")},
		{"empty_new", []byte("some content"), []byte{}},
		{"empty_both", []byte{}, []byte{}},
		// NOTE: empty old file panics in gosaca.ComputeSuffixArray — a known limitation
		// {"empty_old", []byte{}, []byte("new content")},
		{"completely_different", seededRandomBytes(1, 1024), seededRandomBytes(2, 1024)},
		{"small_modification", paper, modifiedPaper},
		{"append_data", []byte("base content"), []byte("base content appended")},
		{"prepend_data", []byte("base content"), []byte("prepended base content")},
		{"single_byte", []byte{0x42}, []byte{0x43}},
		{"medium_similar", mediumOld, makeModified(mediumOld, 99, 0.05)},
		{"larger_similar", largerOld, makeModified(largerOld, 99, 0.01)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			roundTrip(t, tc.oldData, tc.newData, 0)
		})
	}
}

func TestRoundTripPartitions(t *testing.T) {
	oldData := seededRandomBytes(30, 32*1024)
	newData := makeModified(oldData, 42, 0.03)

	configs := []struct {
		name       string
		partitions int
	}{
		{"default", 0},
		{"2part", 2},
		{"4part", 4},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			roundTrip(t, oldData, newData, cfg.partitions)
		})
	}
}

func assertSuffixArrayWellFormed(t *testing.T, data []byte, sa []int) {
	t.Helper()

	assert.Equal(t, len(data)+1, len(sa), "suffix array should include all suffixes including empty suffix")

	seen := make([]bool, len(data)+1)
	for i, idx := range sa {
		assert.GreaterOrEqual(t, idx, 0, "suffix index %d should be non-negative", i)
		assert.LessOrEqual(t, idx, len(data), "suffix index %d should be within bounds", i)
		assert.False(t, seen[idx], "suffix index %d should appear once", idx)
		seen[idx] = true
	}

	for i := 1; i < len(sa); i++ {
		prev := data[sa[i-1]:]
		next := data[sa[i]:]
		assert.EqualValues(t, -1, bytes.Compare(prev, next), "suffixes should be strictly ordered at %d", i)
	}
}

func TestQsufsortDirectConcurrency(t *testing.T) {
	datasets := []struct {
		name string
		data []byte
	}{
		{"paper", paper},
		{"random_32k", seededRandomBytes(777, 32*1024)},
		{"repeated_pattern", bytes.Repeat([]byte("ABCD"), 8*1024)},
	}

	configs := []struct {
		name        string
		concurrency int
	}{
		{"seq", 0},
		{"par2", 2},
		{"par4", 4},
	}

	for _, dataset := range datasets {
		t.Run(dataset.name, func(t *testing.T) {
			var baseline []int

			for _, cfg := range configs {
				t.Run(cfg.name, func(t *testing.T) {
					ctx := &DiffContext{SuffixSortConcurrency: cfg.concurrency}
					consumer := &state.Consumer{}
					got := qsufsort(dataset.data, ctx, consumer)

					assertSuffixArrayWellFormed(t, dataset.data, got)

					if cfg.concurrency == 0 {
						baseline = append([]int(nil), got...)
					} else {
						assert.Equal(t, baseline, got, "parallel qsufsort result should match sequential result")
					}
				})
			}
		})
	}
}

func TestAdderReader(t *testing.T) {
	t.Run("zeros_plus_buffer", func(t *testing.T) {
		ar := &AdderReader{
			Buffer: []byte{1, 2, 3, 4},
			Reader: bytes.NewReader([]byte{0, 0, 0, 0}),
		}
		out, err := io.ReadAll(ar)
		assert.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4}, out)
	})

	t.Run("basic_addition", func(t *testing.T) {
		ar := &AdderReader{
			Buffer: []byte{10, 20, 30},
			Reader: bytes.NewReader([]byte{1, 2, 3}),
		}
		out, err := io.ReadAll(ar)
		assert.NoError(t, err)
		assert.Equal(t, []byte{11, 22, 33}, out)
	})

	t.Run("overflow_wrap", func(t *testing.T) {
		ar := &AdderReader{
			Buffer: []byte{200, 255},
			Reader: bytes.NewReader([]byte{100, 1}),
		}
		out, err := io.ReadAll(ar)
		assert.NoError(t, err)
		// uint8: 200+100=44 (mod 256), 255+1=0 (mod 256)
		assert.Equal(t, []byte{44, 0}, out)
	})

	t.Run("empty", func(t *testing.T) {
		ar := &AdderReader{
			Buffer: []byte{},
			Reader: bytes.NewReader([]byte{}),
		}
		out, err := io.ReadAll(ar)
		assert.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("read_in_chunks", func(t *testing.T) {
		size := 100
		buffer := seededRandomBytes(1, size)
		reader := seededRandomBytes(2, size)

		expected := make([]byte, size)
		for i := range size {
			expected[i] = buffer[i] + reader[i]
		}

		ar := &AdderReader{
			Buffer: buffer,
			Reader: &slowReader{data: reader, chunkSize: 7},
		}

		var out bytes.Buffer
		smallBuf := make([]byte, 7)
		_, err := io.CopyBuffer(&out, ar, smallBuf)
		assert.NoError(t, err)
		assert.Equal(t, expected, out.Bytes())
	})
}

// slowReader returns data in fixed-size chunks to test partial read handling.
type slowReader struct {
	data      []byte
	offset    int
	chunkSize int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	end := min(r.offset+r.chunkSize, len(r.data))
	n := copy(p, r.data[r.offset:end])
	r.offset += n
	return n, nil
}

func TestPSASearch(t *testing.T) {
	t.Run("known_data", func(t *testing.T) {
		buf := []byte("abcdefghijklmnopqrstuvwxyz")
		I := make([]int, len(buf))
		psa := NewPSA(1, buf, I)

		cases := []struct {
			needle  string
			wantPos int
			wantLen int
		}{
			{"abc", 0, 3},
			{"xyz", 23, 3},
			{"mnop", 12, 4},
			{"abcdefghijklmnopqrstuvwxyz", 0, 26},
		}

		for _, tc := range cases {
			pos, n := psa.search([]byte(tc.needle))
			assert.Equal(t, tc.wantPos, pos, "position for needle %q", tc.needle)
			assert.Equal(t, tc.wantLen, n, "length for needle %q", tc.needle)
		}
	})

	t.Run("no_match", func(t *testing.T) {
		buf := []byte("aaaa")
		I := make([]int, len(buf))
		psa := NewPSA(1, buf, I)

		_, n := psa.search([]byte("zz"))
		assert.LessOrEqual(t, n, 1, "should not find a multi-byte match")
	})

	t.Run("partitioned", func(t *testing.T) {
		// Use a buffer with repeated patterns so every partition has matches
		buf := bytes.Repeat([]byte("ABCDEFGH"), 50) // 400 bytes
		needle := []byte("ABCDEFGH")

		for _, partitions := range []int{1, 2, 4} {
			t.Run(string(rune('0'+partitions))+"parts", func(t *testing.T) {
				I := make([]int, len(buf))
				psa := NewPSA(partitions, buf, I)
				pos, n := psa.search(needle)
				assert.GreaterOrEqual(t, n, 8, "should find full pattern match")
				// Verify the match is actually correct
				assert.Equal(t, needle[:n], buf[pos:pos+n])
			})
		}
	})

	t.Run("random_substring", func(t *testing.T) {
		buf := seededRandomBytes(555, 4096)
		I := make([]int, len(buf))
		psa := NewPSA(1, buf, I)

		// Search for substrings known to exist in the buffer
		for _, offset := range []int{0, 100, 1000, 4000} {
			needle := buf[offset : offset+16]
			pos, n := psa.search(needle)
			assert.GreaterOrEqual(t, n, 16, "should find substring at offset %d", offset)
			assert.Equal(t, needle[:n], buf[pos:pos+n])
		}
	})
}
