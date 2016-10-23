package binarydist

import (
	"errors"
	"io"
	"io/ioutil"

	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/wire"
)

// ErrCorrupt indicates that a patch is corrupted, most often that it would produce a longer file
// than specified
var ErrCorrupt = errors.New("corrupt patch")

// Patch applies patch to old, according to the bspatch algorithm,
// and writes the result to new.
func Patch(old io.Reader, new io.Writer, newSize int64, patch wire.ReadContext) error {
	var ctrlOps []pwr.BsdiffControl

	ctrlOp := &pwr.BsdiffControl{}

	for {
		ctrlOp.Reset()
		err := patch.ReadMessage(ctrlOp)
		if err != nil {
			return err
		}

		if ctrlOp.Add == -1 {
			break
		}

		ctrlOps = append(ctrlOps, pwr.BsdiffControl{
			Add:  ctrlOp.Add,
			Seek: ctrlOp.Seek,
			Copy: ctrlOp.Copy,
		})
	}

	diffMessage := &pwr.BsdiffDiff{}
	err := patch.ReadMessage(diffMessage)
	if err != nil {
		return err
	}
	diff := diffMessage.Data

	var diffOffset int64

	extraMessage := &pwr.BsdiffExtra{}
	err = patch.ReadMessage(extraMessage)
	if err != nil {
		return err
	}
	extra := extraMessage.Data

	var extraOffset int64

	obuf, err := ioutil.ReadAll(old)
	if err != nil {
		return err
	}

	nbuf := make([]byte, newSize)

	var oldpos, newpos int64

	for _, ctrl := range ctrlOps {
		// Sanity-check
		if newpos+ctrl.Add > newSize {
			return ErrCorrupt
		}

		// Read diff string
		copy(nbuf[newpos:newpos+ctrl.Add], diff[diffOffset:diffOffset+ctrl.Add])
		diffOffset += ctrl.Add

		// Add old data to diff string
		for i := int64(0); i < ctrl.Add; i++ {
			nbuf[newpos+i] += obuf[oldpos+i]
		}

		// Adjust pointers
		newpos += ctrl.Add
		oldpos += ctrl.Add

		// Sanity-check
		if newpos+ctrl.Copy > newSize {
			return ErrCorrupt
		}

		// Read extra string
		copy(nbuf[newpos:newpos+ctrl.Copy], extra[extraOffset:extraOffset+ctrl.Copy])
		extraOffset += ctrl.Copy

		// Adjust pointers
		newpos += ctrl.Copy
		oldpos += ctrl.Seek
	}

	// Write the new file
	for len(nbuf) > 0 {
		n, err := new.Write(nbuf)
		if err != nil {
			return err
		}
		nbuf = nbuf[n:]
	}

	return nil
}
