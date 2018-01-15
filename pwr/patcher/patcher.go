package patcher

import (
	"fmt"

	"github.com/itchio/wharf/bsdiff"

	"github.com/itchio/savior"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/state"
	"github.com/itchio/wharf/wire"
	"github.com/itchio/wharf/wsync"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/tlc"
)

type savingPatcher struct {
	rctx     *wire.ReadContext
	consumer *state.Consumer

	sc SaveConsumer

	targetContainer *tlc.Container
	sourceContainer *tlc.Container
	header          *pwr.PatchHeader

	rsyncCtx  *wsync.Context
	bsdiffCtx *bsdiff.PatchContext
}

var _ Patcher = (*savingPatcher)(nil)

// New reads the patch header and returns a patcher that
// is ready to Resume, either from the start (nil checkpoint)
// or partway through the patch
func New(patchReader savior.SeekSource, consumer *state.Consumer) (Patcher, error) {
	// Reading the header & both containers is done even
	// when we resume patching partway through (from a checkpoint)
	// Downside: more network usage when resuming
	// Upside: no need to store that on disk

	startOffset, err := patchReader.Resume(nil)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	if startOffset != 0 {
		return nil, errors.Wrap(fmt.Errorf("expected source to resume at 0, got %d", startOffset), 0)
	}

	rawWire := wire.NewReadContext(patchReader)

	// Ensure magic

	err = rawWire.ExpectMagic(pwr.PatchMagic)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// Read header & decompress if needed

	header := &pwr.PatchHeader{}
	err = rawWire.ReadMessage(header)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	rctx, err := pwr.DecompressWire(rawWire, header.Compression)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	// Read both containers

	targetContainer := &tlc.Container{}
	err = rctx.ReadMessage(targetContainer)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	sourceContainer := &tlc.Container{}
	err = rctx.ReadMessage(sourceContainer)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	consumer.Debugf("→ Created patcher")
	consumer.Debugf("before: %s", targetContainer.Stats())
	consumer.Debugf(" after: %s", sourceContainer.Stats())

	sp := &savingPatcher{
		rctx:     rctx,
		consumer: consumer,

		targetContainer: targetContainer,
		sourceContainer: sourceContainer,
		header:          header,
	}

	return sp, nil
}

func (sp *savingPatcher) Resume(c *Checkpoint, targetPool wsync.Pool, bowl bowl.Bowl) error {
	if sp.sc == nil {
		sp.sc = &nopSaveConsumer{}
	}

	consumer := sp.consumer

	if c != nil {
		return errors.Wrap(fmt.Errorf("savingPatcher: Resuming with non-nil checkpoint: stub"), 0)
	}

	c = &Checkpoint{
		FileIndex: 0,
	}

	var numFiles = int64(len(sp.sourceContainer.Files))
	consumer.Debugf("↺ Resuming from file %d / %d", c.FileIndex, numFiles)

	sh := &pwr.SyncHeader{}

	for c.FileIndex < numFiles {
		f := sp.sourceContainer.Files[c.FileIndex]

		consumer.Debugf("Patching file %d: '%s'", c.FileIndex, f.Path)

		err := sp.rctx.ReadMessage(sh)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		consumer.Debugf("Got sync header: %s", sh)

		if sh.FileIndex != c.FileIndex {
			return errors.Wrap(fmt.Errorf("corrupted patch or internal error: expected file %d, got file %d", c.FileIndex, sh.FileIndex), 0)
		}

		switch sh.Type {
		case pwr.SyncHeader_RSYNC:
			c.FileKind = FileKindRsync
		case pwr.SyncHeader_BSDIFF:
			c.FileKind = FileKindBsdiff
		default:
			return errors.Wrap(fmt.Errorf("unknown patch series kind %d for '%s'", sh.Type, f.Path), 0)
		}

		err = sp.processFile(c, targetPool, sh, bowl)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		// reset checkpoint and increment
		c.FileIndex++
		c.RsyncCheckpoint = nil
		c.BsdiffCheckpoint = nil
	}

	return nil
}

func (sp *savingPatcher) processFile(c *Checkpoint, targetPool wsync.Pool, sh *pwr.SyncHeader, bwl bowl.Bowl) error {
	switch c.FileKind {
	case FileKindRsync:
		return sp.processRsync(c, targetPool, sh, bwl)
	case FileKindBsdiff:
		return sp.processBsdiff(c, targetPool, sh, bwl)
	default:
		return errors.Wrap(fmt.Errorf("unknown file kind %d", sh.Type), 0)
	}
}

func (sp *savingPatcher) processRsync(c *Checkpoint, targetPool wsync.Pool, sh *pwr.SyncHeader, bwl bowl.Bowl) error {
	var op *pwr.SyncOp

	if c.RsyncCheckpoint != nil {
		return errors.New("processRsync: restore from checkpoint: stub")
	} else {
		// starting from beginning!

		// let's see if it's a transposition
		op = &pwr.SyncOp{}
		err := sp.rctx.ReadMessage(op)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		if sp.isFullFileOp(sh, op) {
			// oh dang it's either a true no-op, or a rename.
			// either way, we're not troubling the rsync patcher
			// for that.
			sp.consumer.Debugf("Transpose: '%s' -> '%s'",
				sp.targetContainer.Files[op.FileIndex].Path,
				sp.sourceContainer.Files[op.FileIndex].Path,
			)

			err := bwl.Transpose(bowl.Transposition{
				SourceIndex: sh.FileIndex,
				TargetIndex: op.FileIndex,
			})
			if err != nil {
				return errors.Wrap(err, 0)
			}

			// however, we do have to read the end marker
			err = sp.rctx.ReadMessage(op)
			if err != nil {
				return errors.Wrap(err, 0)
			}

			if op.Type != pwr.SyncOp_HEY_YOU_DID_IT {
				return errors.Wrap(fmt.Errorf("corrupt patch: expected HEY_YOU_DID_IT SyncOp, got %s", op.Type), 0)
			}

			return nil
		}
	}

	writer, err := bwl.GetWriter(sh.FileIndex)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	// FIXME: swallowed error
	defer writer.Close()

	if sp.rsyncCtx == nil {
		sp.rsyncCtx = wsync.NewContext(int(pwr.BlockSize))
	}

	if op == nil {
		// we resumed somewhere in the middle, let's initialize op
		op = &pwr.SyncOp{}
	} else {
		// op is non-nil, so we started from scratch and
		// the first op was not a full-file op.
		// We want to relay it now

		wop, err := makeWop(op)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		err = sp.rsyncCtx.ApplySingle(
			writer,
			targetPool,
			wop,
		)
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// let's relay the rest of the messages!
	for {
		if sp.sc.ShouldSave() {
			return errors.New("rsync checkpoints: stub")
		}

		err := sp.rctx.ReadMessage(op)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		if op.Type == pwr.SyncOp_HEY_YOU_DID_IT {
			// hey, we did it!
			return nil
		}

		wop, err := makeWop(op)
		if err != nil {
			return errors.Wrap(err, 0)
		}

		err = sp.rsyncCtx.ApplySingle(
			writer,
			targetPool,
			wop,
		)
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	// ohey, we're done!
	return nil
}

func makeWop(op *pwr.SyncOp) (wsync.Operation, error) {
	switch op.Type {
	case pwr.SyncOp_BLOCK_RANGE:
		return wsync.Operation{
			Type:       wsync.OpBlockRange,
			FileIndex:  op.FileIndex,
			BlockIndex: op.BlockIndex,
			BlockSpan:  op.BlockSpan,
		}, nil
	case pwr.SyncOp_DATA:
		return wsync.Operation{
			Type: wsync.OpData,
			Data: op.Data,
		}, nil
	default:
		return wsync.Operation{}, errors.Wrap(fmt.Errorf("unknown sync op type: %s", op.Type), 0)
	}
}

func (sp *savingPatcher) isFullFileOp(sh *pwr.SyncHeader, op *pwr.SyncOp) bool {
	// only block range ops can be full-file ops
	if op.Type != pwr.SyncOp_BLOCK_RANGE {
		return false
	}

	// and it's gotta start at 0
	if op.BlockIndex != 0 {
		return false
	}

	targetFile := sp.targetContainer.Files[op.FileIndex]
	outputFile := sp.targetContainer.Files[sh.FileIndex]

	// and both files have gotta be the same size
	if targetFile.Size != outputFile.Size {
		return false
	}

	numOutputBlocks := pwr.ComputeNumBlocks(outputFile.Size)

	// and it's gotta, well, span the full file
	if op.BlockSpan != numOutputBlocks {
		return false
	}

	return true
}

func (sp *savingPatcher) processBsdiff(c *Checkpoint, targetPool wsync.Pool, sh *pwr.SyncHeader, bowl bowl.Bowl) error {
	return errors.New("processBsdiff: stub")
}

func (sp *savingPatcher) SetSaveConsumer(sc SaveConsumer) {
	sp.sc = sc
}

func (sp *savingPatcher) GetSourceContainer() *tlc.Container {
	return sp.sourceContainer
}

func (sp *savingPatcher) GetTargetContainer() *tlc.Container {
	return sp.targetContainer
}
