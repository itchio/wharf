package bsdiff

import (
	"fmt"
	"os"
	"runtime"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/itchio/wharf/state"
)

type blockWorkerState struct {
	consumed chan bool
	work     chan int
	matches  chan Match
}

func (ctx *DiffContext) doPartitioned(obuf []byte, obuflen int, nbuf []byte, nbuflen int, memstats *runtime.MemStats, writeMatch WriteMatchFunc, consumer *state.Consumer) error {
	partitions := ctx.Partitions
	if partitions >= len(obuf)-1 {
		partitions = 1
	}

	consumer.ProgressLabel(fmt.Sprintf("Sorting %s...", humanize.IBytes(uint64(obuflen))))
	consumer.Progress(0.0)

	startTime := time.Now()

	pmemstats := &runtime.MemStats{}
	runtime.ReadMemStats(pmemstats)
	oldAlloc := pmemstats.TotalAlloc

	if ctx.I == nil {
		ctx.I = make([]int, len(obuf))
		beforeAlloc := time.Now()
		fmt.Fprintf(os.Stderr, "\nAllocated %d-int I in %s\n", len(obuf), time.Since(beforeAlloc))
	} else {
		for len(ctx.I) < len(obuf) {
			lenBefore := len(ctx.I)
			beforeAlloc := time.Now()
			ctx.I = make([]int, len(obuf))
			fmt.Fprintf(os.Stderr, "\nGrown I from %d to %d in %s\n", lenBefore, len(ctx.I), time.Since(beforeAlloc))
		}
	}

	psa := NewPSA(partitions, obuf, ctx.I)

	runtime.ReadMemStats(pmemstats)
	newAlloc := pmemstats.TotalAlloc
	fmt.Fprintf(os.Stderr, "\nAlloc difference after PSA: %s. Size of I: %s\n", humanize.IBytes(uint64(newAlloc-oldAlloc)), humanize.IBytes(uint64(8*len(psa.I))))

	if ctx.Stats != nil {
		ctx.Stats.TimeSpentSorting += time.Since(startTime)
	}

	if ctx.MeasureMem {
		runtime.ReadMemStats(memstats)
		fmt.Fprintf(os.Stderr, "\nAllocated bytes after qsufsort: %s (%s total)", humanize.IBytes(uint64(memstats.Alloc)), humanize.IBytes(uint64(memstats.TotalAlloc)))
	}

	consumer.ProgressLabel(fmt.Sprintf("Preparing to scan %s...", humanize.IBytes(uint64(nbuflen))))
	consumer.Progress(0.0)

	startTime = time.Now()

	analyzeBlock := func(nbuflen int, nbuf []byte, offset int, matches chan Match) {
		var lenf int

		// Compute the differences, writing ctrl as we go
		var scan, pos, length int
		var lastscan, lastpos, lastoffset int

		for scan < nbuflen {
			var oldscore int
			scan += length

			for scsc := scan; scan < nbuflen; scan++ {
				pos, length = psa.search(nbuf[scan:])

				for ; scsc < scan+length; scsc++ {
					if scsc+lastoffset < obuflen &&
						obuf[scsc+lastoffset] == nbuf[scsc] {
						oldscore++
					}
				}

				if (length == oldscore && length != 0) || length > oldscore+8 {
					break
				}

				if scan+lastoffset < obuflen && obuf[scan+lastoffset] == nbuf[scan] {
					oldscore--
				}
			}

			if length != oldscore || scan == nbuflen {
				var s, Sf int
				lenf = 0
				for i := int(0); lastscan+i < scan && lastpos+i < obuflen; {
					if obuf[lastpos+i] == nbuf[lastscan+i] {
						s++
					}
					i++
					if s*2-i > Sf*2-lenf {
						Sf = s
						lenf = i
					}
				}

				lenb := 0
				if scan < nbuflen {
					var s, Sb int
					for i := int(1); (scan >= lastscan+i) && (pos >= i); i++ {
						if obuf[pos-i] == nbuf[scan-i] {
							s++
						}
						if s*2-i > Sb*2-lenb {
							Sb = s
							lenb = i
						}
					}
				}

				if lastscan+lenf > scan-lenb {
					overlap := (lastscan + lenf) - (scan - lenb)
					s := int(0)
					Ss := int(0)
					lens := int(0)
					for i := int(0); i < overlap; i++ {
						if nbuf[lastscan+lenf-overlap+i] == obuf[lastpos+lenf-overlap+i] {
							s++
						}
						if nbuf[scan-lenb+i] == obuf[pos-lenb+i] {
							s--
						}
						if s > Ss {
							Ss = s
							lens = i + 1
						}
					}

					lenf += lens - overlap
					lenb -= lens
				}

				m := Match{
					addOldStart: lastpos,
					addNewStart: lastscan + offset,
					addLength:   lenf,
					copyEnd:     scan - lenb + offset,
				}

				if m.addLength > 0 || (m.copyEnd != m.copyStart()) {
					// if not a no-op, send
					matches <- m
				}

				lastscan = scan - lenb
				lastpos = pos - lenb
				lastoffset = pos - scan
			}
		}

		matches <- Match{eoc: true}
	}

	blockSize := 128 * 1024
	numBlocks := (nbuflen + blockSize - 1) / blockSize

	if numBlocks < partitions {
		blockSize = nbuflen / partitions
		numBlocks = (nbuflen + blockSize - 1) / blockSize
	}

	numWorkers := partitions * 12
	if numWorkers > numBlocks {
		numWorkers = numBlocks
	}

	blockWorkersState := make([]blockWorkerState, numWorkers)

	// initialize all channels
	for i := 0; i < numWorkers; i++ {
		blockWorkersState[i].work = make(chan int, 1)
		blockWorkersState[i].matches = make(chan Match, 256)
		blockWorkersState[i].consumed = make(chan bool, 1)
		blockWorkersState[i].consumed <- true
	}

	for i := 0; i < numWorkers; i++ {
		go func(workerState blockWorkerState, workerIndex int) {
			for blockIndex := range workerState.work {
				boundary := blockSize * blockIndex
				realBlockSize := blockSize
				if blockIndex == numBlocks-1 {
					realBlockSize = nbuflen - boundary
				}

				analyzeBlock(realBlockSize, nbuf[boundary:boundary+realBlockSize], boundary, workerState.matches)
			}
		}(blockWorkersState[i], i)
	}

	go func() {
		workerIndex := 0

		for i := 0; i < numBlocks; i++ {
			<-blockWorkersState[workerIndex].consumed
			blockWorkersState[workerIndex].work <- i

			workerIndex = (workerIndex + 1) % numWorkers
		}

		for workerIndex := 0; workerIndex < numWorkers; workerIndex++ {
			close(blockWorkersState[workerIndex].work)
		}
		// fmt.Fprintf(os.Stderr, "Sent all blockworks\n")
	}()

	if ctx.MeasureMem {
		runtime.ReadMemStats(memstats)
		fmt.Fprintf(os.Stderr, "\nAllocated bytes after scan-prepare: %s (%s total)", humanize.IBytes(uint64(memstats.Alloc)), humanize.IBytes(uint64(memstats.TotalAlloc)))
	}

	consumer.ProgressLabel(fmt.Sprintf("Scanning %s (%d blocks of %s)...", humanize.IBytes(uint64(nbuflen)), numBlocks, humanize.IBytes(uint64(blockSize))))

	{
		workerIndex := 0
		for blockIndex := 0; blockIndex < numBlocks; blockIndex++ {
			consumer.Progress(float64(blockIndex) / float64(numBlocks))
			state := blockWorkersState[workerIndex]

			for match := range state.matches {
				if match.eoc {
					break
				}

				writeMatch(match)
			}

			state.consumed <- true
			workerIndex = (workerIndex + 1) % numWorkers
		}
	}

	if ctx.Stats != nil {
		ctx.Stats.TimeSpentScanning += time.Since(startTime)
	}

	if ctx.MeasureMem {
		runtime.ReadMemStats(memstats)
		consumer.Debugf("\nAllocated bytes after scan: %s (%s total)", humanize.IBytes(uint64(memstats.Alloc)), humanize.IBytes(uint64(memstats.TotalAlloc)))
		fmt.Fprintf(os.Stderr, "\nAllocated bytes after scan: %s (%s total)", humanize.IBytes(uint64(memstats.Alloc)), humanize.IBytes(uint64(memstats.TotalAlloc)))
	}

	return nil
}
