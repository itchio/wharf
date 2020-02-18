package patcher

import (
	"fmt"

	"github.com/itchio/lake"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/wire"
)

// Checkpoint contains state information for a patcher that
// can be written to disk and read back from disk to resume
// roughly where we left off.
type Checkpoint struct {
	MessageCheckpoint *wire.MessageReaderCheckpoint

	FileIndex int64
	FileKind  FileKind

	BowlCheckpoint   *bowl.BowlCheckpoint
	SyncHeader       *pwr.SyncHeader
	RsyncCheckpoint  *RsyncCheckpoint
	BsdiffCheckpoint *BsdiffCheckpoint
}

// FileKind denotes either rsync or bsdiff patching
type FileKind int

const (
	// FileKindRsync denotes rsync patching (blocks-based)
	FileKindRsync = 1
	// FileKindBsdiff denotes bsdiff patching (addition-based)
	FileKindBsdiff = 2
)

// RsyncCheckpoint is used when saving a patcher checkpoint in the middle
// of patching a file with rsync instructions.
type RsyncCheckpoint struct {
	WriterCheckpoint *bowl.WriterCheckpoint
}

// BsdiffCheckpoint is used when saving a patcher checkpoint in the middle
// of patching a file with bsdiff instructions.
type BsdiffCheckpoint struct {
	WriterCheckpoint *bowl.WriterCheckpoint

	// instructions in bsdiff are relative seeks, so we need to keep track of
	// the offset in the (single) target file
	OldOffset int64

	// bsdiff series are applied against a single target file, and its index
	// is in a past message, so we need to keep track of it
	TargetIndex int64
}

// A Patcher applies a wharf patch, either standard (rsync-only) or optimized
// (rsync + bsdiff). It can save its progress and resume.
// It patches to a bowl: fresh bowls (create new folder with new build), overlay
// bowls (patch to overlay, then commit that overlay in-place), etc.
type Patcher interface {
	SetSaveConsumer(sc SaveConsumer)
	Resume(checkpoint *Checkpoint, targetPool lake.Pool, bowl bowl.Bowl) error
	Progress() float64

	GetSourceContainer() *tlc.Container
	GetTargetContainer() *tlc.Container
	SetSourceIndexWhitelist(sourceIndexWhitelist map[int64]bool)
	GetTouchedFiles() int64
}

// AfterSaveAction describes what the patcher should do after it saved.
// This can be used to gracefully stop it.
type AfterSaveAction int

const (
	// AfterSaveContinue indicates that the patcher should continue after saving.
	AfterSaveContinue AfterSaveAction = 1
	// AfterSaveStop indicates that the patcher should stop and return ErrStop
	AfterSaveStop AfterSaveAction = 2
)

// A SaveConsumer can be set on a Patcher to decide if the patcher should save
// (whenever it reaches a convenient place for a checkpoint), and to receive
// the checkpoints, and let the patcher know if it should stop or continue
type SaveConsumer interface {
	ShouldSave() bool
	Save(c *Checkpoint) (AfterSaveAction, error)
}

// ErrStop is returned by `patcher.Resume` if it just saved a checkpoint
// and the `SaveConsumer` returned `AfterSaveStop`.
var ErrStop = fmt.Errorf("patching was stopped after save!")

// nopSaveConsumer

type nopSaveConsumer struct{}

var _ SaveConsumer = (*nopSaveConsumer)(nil)

func (nsc *nopSaveConsumer) ShouldSave() bool {
	return false
}

func (nsc *nopSaveConsumer) Save(c *Checkpoint) (AfterSaveAction, error) {
	return AfterSaveContinue, nil
}
