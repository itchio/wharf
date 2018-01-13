package bowl

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/tlc"
)

type dryBowl struct {
	SourceContainer *tlc.Container
	TargetContainer *tlc.Container
}

var _ Bowl = (*dryBowl)(nil)

type DryBowlParams struct {
	SourceContainer *tlc.Container
	TargetContainer *tlc.Container
}

// NewDryBowl returns a bowl that throws away all writes
func NewDryBowl(params *DryBowlParams) (Bowl, error) {
	// input validation

	if params.TargetContainer == nil {
		return nil, errors.New("drybowl: TargetContainer must not be nil")
	}

	if params.SourceContainer == nil {
		return nil, errors.New("drybowl: SourceContainer must not be nil")
	}

	return &dryBowl{
		SourceContainer: params.SourceContainer,
		TargetContainer: params.TargetContainer,
	}, nil
}

func (db *dryBowl) GetWriter(index int64) (io.WriteCloser, error) {
	if index < 0 || index >= int64(len(db.SourceContainer.Files)) {
		return nil, errors.Wrap(fmt.Errorf("drybowl: invalid source index %d", index), 0)
	}

	// throw away the writes. alll the writes.
	return &nopWriteCloser{ioutil.Discard}, nil
}

func (db *dryBowl) Transpose(t Transposition) error {
	if t.SourceIndex < 0 || t.SourceIndex >= int64(len(db.SourceContainer.Files)) {
		return errors.Wrap(fmt.Errorf("drybowl: invalid source index %d", t.SourceIndex), 0)
	}
	if t.TargetIndex < 0 || t.TargetIndex >= int64(len(db.TargetContainer.Files)) {
		return errors.Wrap(fmt.Errorf("drybowl: invalid target index %d", t.TargetIndex), 0)
	}

	// muffin to do
	return nil
}

func (db *dryBowl) Commit() error {
	// literally nothing to do, we're just throwing stuff away!
	return nil
}

// nopWriteCloser

type nopWriteCloser struct {
	w io.Writer
}

var _ io.WriteCloser = (*nopWriteCloser)(nil)

func (nwc *nopWriteCloser) Write(buf []byte) (int, error) {
	return nwc.w.Write(buf)
}

func (nwc *nopWriteCloser) Close() error {
	return nil
}
