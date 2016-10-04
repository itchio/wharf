package pwr

import (
	"archive/zip"
	"fmt"
	"io"
	"strings"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/eos"
	"github.com/itchio/wharf/pools/fspool"
	"github.com/itchio/wharf/pools/zippool"
	"github.com/itchio/wharf/tlc"
	"github.com/itchio/wharf/wsync"
)

type Healer interface {
	WoundsConsumer

	SetNumWorkers(int)
	TotalHealed() int64
}

func NewHealer(spec string, target string) (Healer, error) {
	tokens := strings.SplitN(spec, ",", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Invalid healer spec: expected 'type,url' but got '%s'", spec)
	}

	healerType := tokens[0]
	healerURL := tokens[1]

	switch healerType {
	case "archive":
		file, err := eos.Open(healerURL)

		ah := &ArchiveHealer{
			File:   file,
			Target: target,
		}
		return ah, nil
	case "manifest":
		return nil, fmt.Errorf("Manifest healer: stub")
	}

	return nil, fmt.Errorf("Unknown healer type %s", healerType)
}

// ArchiveHealer

type ArchiveHealer struct {
	// the directory we should heal
	Target string

	// the file
	File eos.File

	// number of workers running in parallel
	NumWorkers int

	// internal
	totalCorrupted int64
	totalHealed    int64
}

var _ Healer = (*ArchiveHealer)(nil)

func (ah *ArchiveHealer) Do(container *tlc.Container, wounds chan *Wound) error {
	files := make(map[int64]bool)
	fileIndices := make(chan int64)

	stat, err := ah.File.Stat()
	if err != nil {
		return err
	}

	zipReader, err := zip.NewReader(ah.File, stat.Size())
	if err != nil {
		return errors.Wrap(err, 1)
	}

	err = container.Prepare(ah.Target)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	targetPool := fspool.New(container, ah.Target)

	errs := make(chan error)
	done := make(chan bool)

	for i := 0; i < ah.NumWorkers; i++ {
		go ah.heal(container, zipReader, stat.Size(), targetPool, fileIndices, errs, done)
	}

	for wound := range wounds {
		ah.totalCorrupted += wound.Size()

		if files[wound.FileIndex] {
			// already queued
			continue
		}

		files[wound.FileIndex] = true
		fileIndices <- wound.FileIndex
	}

	close(fileIndices)

	for i := 0; i < ah.NumWorkers; i++ {
		select {
		case err = <-errs:
			return errors.Wrap(err, 1)
		case <-done:
			// good!
		}
	}

	err = ah.File.Close()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	return nil
}

func (ah *ArchiveHealer) heal(container *tlc.Container, zipReader *zip.Reader, zipSize int64, targetPool wsync.WritablePool,
	fileIndices chan int64, errs chan error, done chan bool) {

	var sourcePool wsync.Pool
	var err error

	sourcePool = zippool.New(container, zipReader)

	for fileIndex := range fileIndices {
		var reader io.Reader
		var writer io.WriteCloser

		reader, err = sourcePool.GetReader(fileIndex)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		writer, err = targetPool.GetWriter(fileIndex)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		_, err = io.Copy(writer, reader)
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}

		err = writer.Close()
		if err != nil {
			errs <- errors.Wrap(err, 1)
			return
		}
	}

	err = sourcePool.Close()
	if err != nil {
		errs <- errors.Wrap(err, 1)
		return
	}

	done <- true
}

func (ah *ArchiveHealer) TotalCorrupted() int64 {
	return ah.totalCorrupted
}

func (ah *ArchiveHealer) TotalHealed() int64 {
	return ah.totalHealed
}

func (ah *ArchiveHealer) SetNumWorkers(numWorkers int) {
	ah.NumWorkers = numWorkers
}
