package pwr

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"path/filepath"

	humanize "github.com/dustin/go-humanize"
	"github.com/go-errors/errors"
	"github.com/itchio/wharf/bsdiff"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
	"github.com/itchio/wharf/wsync"
)

// FileOrigin maps a target's file index to how many bytes it
// contribute to a given source file
type FileOrigin map[int64]int64

// DiffMappings stores correspondances between files - source files are mapped
// to the target file that has the most blocks in common, or has the same name
type DiffMappings map[int64]*DiffMapping

type DiffMapping struct {
	TargetIndex int64
	NumBytes    int64
}

type WriteMatchesFunc func(matches []bsdiff.Match)

type RediffTask struct {
	sourceFileIndex int64
	obuf            []byte
	nbuf            []byte
}

func (dm DiffMappings) ToString(sourceContainer tlc.Container, targetContainer tlc.Container) string {
	s := ""
	for sourceIndex, diffMapping := range dm {
		s += fmt.Sprintf("%s <- %s (%s in common)\n",
			sourceContainer.Files[sourceIndex].Path,
			targetContainer.Files[diffMapping.TargetIndex].Path,
			humanize.IBytes(uint64(diffMapping.NumBytes)),
		)
	}
	return s
}

type Timeline struct {
	Groups []TimelineGroup `json:"groups"`
	Items  []TimelineItem  `json:"items"`
}

type TimelineGroup struct {
	ID      int    `json:"id"`
	Content string `json:"content"`
}

type TimelineItem struct {
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Content string  `json:"content"`
	Style   string  `json:"style"`
	Title   string  `json:"title"`
	Group   int     `json:"group"`
}

type RediffContext struct {
	SourcePool wsync.Pool
	TargetPool wsync.Pool

	// optional
	SuffixSortConcurrency int
	Partitions            int
	Compression           *CompressionSettings
	Consumer              *state.Consumer
	BsdiffStats           *bsdiff.DiffStats
	Timeline              *Timeline

	// set on Analyze
	TargetContainer *tlc.Container
	SourceContainer *tlc.Container

	// internal
	DiffMappings DiffMappings
	MeasureMem   bool
	NumWorkers   int
}

func (rc *RediffContext) AnalyzePatch(patchReader io.Reader) error {
	var err error

	rctx := wire.NewReadContext(patchReader)

	err = rctx.ExpectMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	ph := &PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	rctx, err = DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	rc.TargetContainer = targetContainer

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}
	rc.SourceContainer = sourceContainer

	rop := &SyncOp{}

	targetPathsToIndex := make(map[string]int64)
	for targetFileIndex, file := range targetContainer.Files {
		targetPathsToIndex[file.Path] = int64(targetFileIndex)
	}

	rc.DiffMappings = make(DiffMappings)

	var doneBytes int64

	sh := &SyncHeader{}

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		sh.Reset()
		err = rctx.ReadMessage(sh)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		if sh.FileIndex != int64(sourceFileIndex) {
			return errors.Wrap(fmt.Errorf("Malformed patch, expected index %d, got %d", sourceFileIndex, sh.FileIndex), 1)
		}

		rc.Consumer.ProgressLabel(sourceFile.Path)
		rc.Consumer.Progress(float64(doneBytes) / float64(sourceContainer.Size))

		bytesReusedPerFileIndex := make(FileOrigin)
		readingOps := true
		var numBlockRange int64
		var numData int64

		for readingOps {
			rop.Reset()
			err = rctx.ReadMessage(rop)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			switch rop.Type {
			case SyncOp_BLOCK_RANGE:
				numBlockRange++
				alreadyReused := bytesReusedPerFileIndex[rop.FileIndex]
				lastBlockIndex := rop.BlockIndex + rop.BlockSpan
				targetFile := targetContainer.Files[rop.FileIndex]
				lastBlockSize := ComputeBlockSize(targetFile.Size, lastBlockIndex)
				otherBlocksSize := BlockSize*rop.BlockSpan - 1

				bytesReusedPerFileIndex[rop.FileIndex] = alreadyReused + otherBlocksSize + lastBlockSize

			case SyncOp_DATA:
				numData++

			default:
				switch rop.Type {
				case SyncOp_HEY_YOU_DID_IT:
					readingOps = false
				default:
					return errors.Wrap(ErrMalformedPatch, 1)
				}
			}
		}

		if numBlockRange == 1 && numData == 0 {
			// transpositions (renames, etc.) don't need bsdiff'ing :)
		} else {
			var diffMapping *DiffMapping

			for targetFileIndex, numBytes := range bytesReusedPerFileIndex {
				targetFile := targetContainer.Files[targetFileIndex]
				// first, better, or equal target file with same name (prefer natural mappings)
				if diffMapping == nil || numBytes > diffMapping.NumBytes || (numBytes == diffMapping.NumBytes && targetFile.Path == sourceFile.Path) {
					diffMapping = &DiffMapping{
						TargetIndex: targetFileIndex,
						NumBytes:    numBytes,
					}
				}
			}

			if diffMapping == nil {
				// even without any common blocks, bsdiff might still be worth it
				// if the file is named the same
				if samePathTargetFileIndex, ok := targetPathsToIndex[sourceFile.Path]; ok {
					targetFile := targetContainer.Files[samePathTargetFileIndex]

					// don't take into account files that were 0 bytes (it happens). bsdiff won't like that.
					if targetFile.Size > 0 {
						diffMapping = &DiffMapping{
							TargetIndex: samePathTargetFileIndex,
							NumBytes:    0,
						}
					}
				}
			}

			if diffMapping != nil {
				rc.DiffMappings[int64(sourceFileIndex)] = diffMapping
			}
		}

		doneBytes += sourceFile.Size
	}

	return nil
}

func (rc *RediffContext) OptimizePatch(patchReader io.Reader, patchWriter io.Writer) error {
	var err error

	if rc.SourcePool == nil {
		return errors.Wrap(fmt.Errorf("SourcePool cannot be nil"), 1)
	}

	if rc.TargetPool == nil {
		return errors.Wrap(fmt.Errorf("TargetPool cannot be nil"), 1)
	}

	if rc.DiffMappings == nil {
		return errors.Wrap(fmt.Errorf("AnalyzePatch must be called before OptimizePatch"), 1)
	}

	rctx := wire.NewReadContext(patchReader)
	wctx := wire.NewWriteContext(patchWriter)

	err = wctx.WriteMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	err = rctx.ExpectMagic(PatchMagic)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	ph := &PatchHeader{}
	err = rctx.ReadMessage(ph)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	compression := rc.Compression
	if compression == nil {
		compression = defaultRediffCompressionSettings()
	}

	wph := &PatchHeader{
		Compression: compression,
	}
	err = wctx.WriteMessage(wph)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	rctx, err = DecompressWire(rctx, ph.Compression)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	wctx, err = CompressWire(wctx, wph.Compression)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	err = wctx.WriteMessage(targetContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	err = wctx.WriteMessage(sourceContainer)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	sh := &SyncHeader{}
	bh := &BsdiffHeader{}
	rop := &SyncOp{}

	numWorkers := rc.NumWorkers
	if numWorkers == 0 {
		numWorkers = 1
	}

	rc.Consumer.Infof("Using %d workers", numWorkers)

	contexts := make([]*bsdiff.DiffContext, numWorkers)

	if rc.Timeline != nil {
		rc.Timeline.Groups = append(rc.Timeline.Groups, TimelineGroup{
			ID:      -1,
			Content: "Orchestrator",
		})
	}

	for workerIndex := 0; workerIndex < numWorkers; workerIndex++ {
		contexts[workerIndex] = &bsdiff.DiffContext{
			SuffixSortConcurrency: rc.SuffixSortConcurrency,
			Partitions:            rc.Partitions,
			Stats:                 rc.BsdiffStats,
			MeasureMem:            rc.MeasureMem,
		}
		if rc.Timeline != nil {
			rc.Timeline.Groups = append(rc.Timeline.Groups, TimelineGroup{
				ID:      workerIndex,
				Content: fmt.Sprintf("Worker %d", workerIndex),
			})
		}
	}

	initialStart := time.Now()

	var biggestSourceFile int64
	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		diffMapping := rc.DiffMappings[int64(sourceFileIndex)]
		if diffMapping == nil {
			continue
		}

		if sourceFile.Size > biggestSourceFile {
			biggestSourceFile = sourceFile.Size
		}
	}

	allMatches := make(map[int64][]bsdiff.Match)
	var allMatchesLock sync.Mutex
	tasks := make(chan RediffTask, numWorkers)
	workerErrs := make(chan error, numWorkers)

	for workerIndex := 0; workerIndex < numWorkers; workerIndex++ {
		go func(workerIndex int) {
			bdc := contexts[workerIndex]

			for task := range tasks {
				var matches []bsdiff.Match
				startTime := time.Now()

				err := bdc.Do(bytes.NewReader(task.obuf), bytes.NewReader(task.nbuf), func(m bsdiff.Match) {
					matches = append(matches, m)
				}, rc.Consumer)

				endTime := time.Now()

				if rc.Timeline != nil {
					sourceFile := sourceContainer.Files[task.sourceFileIndex]

					heat := int(float64(sourceFile.Size) / float64(biggestSourceFile) * 240.0)
					rc.Timeline.Items = append(rc.Timeline.Items, TimelineItem{
						Content: filepath.Base(sourceFile.Path),
						Style:   fmt.Sprintf("background-color: hsl(%d, 100%%, 50%%)", heat),
						Title:   fmt.Sprintf("%s %s", humanize.IBytes(uint64(sourceFile.Size)), sourceFile.Path),
						Start:   startTime.Sub(initialStart).Seconds(),
						End:     endTime.Sub(initialStart).Seconds(),
						Group:   workerIndex,
					})
				}

				if err != nil {
					workerErrs <- err
					return
				}

				allMatchesLock.Lock()
				allMatches[int64(task.sourceFileIndex)] = matches
				allMatchesLock.Unlock()
			}
			workerErrs <- nil
		}(workerIndex)
	}

	for sourceFileIndex := range sourceContainer.Files {
		diffMapping := rc.DiffMappings[int64(sourceFileIndex)]

		if diffMapping == nil {
			continue
		}

		sourceFileReader, err := rc.SourcePool.GetReadSeeker(int64(sourceFileIndex))
		if err != nil {
			return errors.Wrap(err, 0)
		}

		targetFileReader, err := rc.TargetPool.GetReadSeeker(diffMapping.TargetIndex)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = sourceFileReader.Seek(0, os.SEEK_SET)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		_, err = targetFileReader.Seek(0, os.SEEK_SET)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		obuf, err := ioutil.ReadAll(targetFileReader)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		nbuf, err := ioutil.ReadAll(sourceFileReader)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		tasks <- RediffTask{
			obuf:            obuf,
			nbuf:            nbuf,
			sourceFileIndex: int64(sourceFileIndex),
		}
	}

	close(tasks)

	for workerIndex := 0; workerIndex < numWorkers; workerIndex++ {
		err = <-workerErrs
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	serializeStartTime := time.Now()

	for sourceFileIndex, sourceFile := range sourceContainer.Files {
		sh.Reset()
		err = rctx.ReadMessage(sh)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		if sh.FileIndex != int64(sourceFileIndex) {
			return errors.Wrap(fmt.Errorf("Malformed patch, expected index %d, got %d", sourceFileIndex, sh.FileIndex), 1)
		}

		diffMapping := rc.DiffMappings[int64(sourceFileIndex)]

		if diffMapping == nil {
			// if no mapping, just copy ops straight up
			err = wctx.WriteMessage(sh)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			for {
				rop.Reset()
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.Wrap(err, 0)
				}

				if rop.Type == SyncOp_HEY_YOU_DID_IT {
					break
				}

				err = wctx.WriteMessage(rop)
				if err != nil {
					return errors.Wrap(err, 0)
				}
			}
		} else {
			// signal bsdiff start to patcher
			sh.Reset()
			sh.FileIndex = int64(sourceFileIndex)
			sh.Type = SyncHeader_BSDIFF
			err = wctx.WriteMessage(sh)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			bh.Reset()
			bh.TargetIndex = diffMapping.TargetIndex
			err = wctx.WriteMessage(bh)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			// throw away old ops
			for {
				err = rctx.ReadMessage(rop)
				if err != nil {
					return errors.Wrap(err, 0)
				}

				if rop.Type == SyncOp_HEY_YOU_DID_IT {
					break
				}
			}

			// then bsdiff
			sourceFileReader, err := rc.SourcePool.GetReadSeeker(int64(sourceFileIndex))
			if err != nil {
				return errors.Wrap(err, 0)
			}

			targetFileReader, err := rc.TargetPool.GetReadSeeker(diffMapping.TargetIndex)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			_, err = sourceFileReader.Seek(0, os.SEEK_SET)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			_, err = targetFileReader.Seek(0, os.SEEK_SET)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			rc.Consumer.ProgressLabel(sourceFile.Path)

			matches := allMatches[int64(sourceFileIndex)]

			bdc := contexts[0]

			err = bdc.WriteMessages(targetFileReader, sourceFileReader, matches, wctx.WriteMessage)
			if err != nil {
				return errors.Wrap(err, 0)
			}
		}

		// and don't forget to indicate success
		rop.Reset()
		rop.Type = SyncOp_HEY_YOU_DID_IT

		err = wctx.WriteMessage(rop)
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	serializeEndTime := time.Now()

	if rc.Timeline != nil {
		rc.Timeline.Items = append(rc.Timeline.Items, TimelineItem{
			Content: "Serializing all",
			Style:   "background-color: black",
			Start:   serializeStartTime.Sub(initialStart).Seconds(),
			End:     serializeEndTime.Sub(initialStart).Seconds(),
			Group:   -1,
		})
	}

	err = wctx.Close()
	if err != nil {
		return errors.Wrap(err, 0)
	}

	return nil
}

func defaultRediffCompressionSettings() *CompressionSettings {
	return &CompressionSettings{
		Algorithm: CompressionAlgorithm_ZSTD,
		Quality:   9,
	}
}
