package patcher

import (
	"fmt"

	"github.com/itchio/wharf/pwr/bowl"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wire"
	"github.com/itchio/wharf/wsync"
)

type Checkpoint struct {
	MessageCheckpoint *wire.MessageReaderCheckpoint

	FileIndex int64
	FileKind  FileKind

	RsyncCheckpoint  *RsyncCheckpoint
	BsdiffCheckpoint *BsdiffCheckpoint
}

type FileKind int

const (
	FileKindRsync  = 1
	FileKindBsdiff = 2
)

type RsyncCheckpoint struct {
	OutputOffset int64
}

type BsdiffCheckpoint struct {
	OutputOffset int64
	OldOffset    int64
}

type Patcher interface {
	SetSaveConsumer(sc SaveConsumer)
	Resume(checkpoint *Checkpoint, targetPool wsync.Pool, bowl bowl.Bowl) error

	GetSourceContainer() *tlc.Container
	GetTargetContainer() *tlc.Container
}

type AfterSaveAction int

const (
	AfterSaveContinue AfterSaveAction = 1
	AfterSaveStop     AfterSaveAction = 2
)

type SaveConsumer interface {
	ShouldSave() bool
	Save(c *Checkpoint) (AfterSaveAction, error)
}

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
