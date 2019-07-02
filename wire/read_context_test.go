package wire_test

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/itchio/headway/united"
	"github.com/itchio/savior/brotlisource"

	"github.com/itchio/go-brotli/enc"
	"github.com/itchio/savior/seeksource"

	"github.com/itchio/wharf/wire"
	"github.com/stretchr/testify/assert"
)

const magic int32 = 0xfad0fad

type shouldCheckpointFunc func(index int) bool
type scenario struct {
	name             string
	shouldCheckpoint shouldCheckpointFunc
}

func Test_ReadContext(t *testing.T) {
	scenarios := []scenario{
		scenario{
			name: "none",
			shouldCheckpoint: func(i int) bool {
				return false
			},
		},
		scenario{
			name: "all",
			shouldCheckpoint: func(i int) bool {
				return true
			},
		},

		scenario{
			name: "even",
			shouldCheckpoint: func(i int) bool {
				return i%2 == 0
			},
		},
	}

	qualities := []int{
		1,
		3,
		6,
		9,
	}

	for _, quality := range qualities {
		buf := new(bytes.Buffer)

		bw := enc.NewBrotliWriter(buf, &enc.BrotliWriterOptions{
			Quality: quality,
		})

		w := wire.NewWriteContext(bw)
		writeSampleMessages(t, w)

		must(t, w.Close())
		log.Printf("Q%d payload size: %s", quality, united.FormatBytes(int64(buf.Len())))

		for _, scenario := range scenarios {
			t.Run(fmt.Sprintf("%s-q%d", scenario.name, quality), func(t *testing.T) {
				source := seeksource.FromBytes(buf.Bytes())
				bs := brotlisource.New(source)

				r := wire.NewReadContext(bs)

				must(t, r.Resume(nil))
				must(t, r.ExpectMagic(magic))

				msg := &wire.Sample{}
				totalMessages := 0
				totalCheckpoints := 0
				i := 0
				for {
					if scenario.shouldCheckpoint(i) {
						r.WantSave()
					}
					i++

					totalMessages++
					must(t, r.ReadMessage(msg))
					if msg.Eof {
						break
					}

					c := r.PopCheckpoint()
					if c != nil {
						totalCheckpoints++

						r = wire.NewReadContext(bs)
						must(t, r.Resume(c))
					}
				}
				log.Printf("Read %d messages, had %d checkpoints", totalMessages, totalCheckpoints)
			})
		}
	}
}

func writeSampleMessages(t *testing.T, w *wire.WriteContext) {
	rng := rand.New(rand.NewSource(0xd00d627))
	must(t, w.WriteMagic(magic))

	for i := 0; i < 64; i++ {
		datalen := (256 + rng.Intn(256)) * 1024
		data := make([]byte, datalen)
		for j := 0; j < datalen; j++ {
			data[j] = byte(rng.Intn(256))
		}

		msg := &wire.Sample{
			Data:   data,
			Number: int64(i),
		}
		must(t, w.WriteMessage(msg))
	}

	must(t, w.WriteMessage(&wire.Sample{Eof: true}))
}

func must(t *testing.T, err error) {
	if err != nil {
		assert.NoError(t, err)
		t.FailNow()
	}
}
