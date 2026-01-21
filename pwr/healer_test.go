package pwr

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/itchio/arkive/zip"
	"github.com/itchio/headway/state"
	"github.com/itchio/lake/pools"
	"github.com/itchio/lake/tlc"
	"github.com/itchio/randsource"
	"github.com/itchio/wharf/wtest"
	"github.com/stretchr/testify/assert"
)

func Test_NewHealer(t *testing.T) {
	_, err := NewHealer("", "/dev/null")
	assert.Error(t, err)

	_, err = NewHealer("nope,/dev/null", "invalid")
	assert.Error(t, err)

	healer, err := NewHealer("archive,/dev/null", "invalid")
	assert.NoError(t, err)

	_, ok := healer.(*ArchiveHealer)
	assert.True(t, ok)
}

type healMethod func()

func Test_ArchiveHealer(t *testing.T) {
	mainDir, err := os.MkdirTemp("", "archivehealer")
	assert.NoError(t, err)
	defer os.RemoveAll(mainDir)

	archivePath := filepath.Join(mainDir, "archive.zip")
	archiveWriter, err := os.Create(archivePath)
	assert.NoError(t, err)
	defer archiveWriter.Close()

	targetDir := filepath.Join(mainDir, "target")
	assert.NoError(t, os.MkdirAll(targetDir, 0755))

	zw := zip.NewWriter(archiveWriter)
	numFiles := 16

	prng := randsource.Reader{
		Source: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	fakeData, err := io.ReadAll(io.LimitReader(prng, 4*1024))
	wtest.Must(t, err)

	nameFor := func(index int) string {
		return fmt.Sprintf("file-%d", index)
	}

	pathFor := func(index int) string {
		return filepath.Join(targetDir, nameFor(index))
	}

	for i := 0; i < numFiles; i++ {
		fh := &zip.FileHeader{
			Name: nameFor(i),
		}
		fh.Method = zip.Deflate

		writer, cErr := zw.CreateHeader(fh)
		assert.NoError(t, cErr)

		_, cErr = writer.Write(fakeData)
		assert.NoError(t, cErr)
	}

	assert.NoError(t, zw.Close())

	container, err := tlc.WalkAny(archivePath, tlc.WalkOpts{})
	assert.NoError(t, err)

	pool, err := pools.New(container, archivePath)
	wtest.Must(t, err)

	consumer := &state.Consumer{}

	hashes, err := ComputeSignature(context.Background(), container, pool, consumer)
	wtest.Must(t, err)

	sigInfo := &SignatureInfo{
		Container: container,
		Hashes:    hashes,
	}

	healDirect := func() {
		healer, err := NewHealer(fmt.Sprintf("archive,%s", archivePath), targetDir)
		assert.NoError(t, err)

		wounds := make(chan *Wound)
		done := make(chan bool)

		go func() {
			err := healer.Do(context.Background(), container, wounds)
			assert.NoError(t, err)
			done <- true
		}()

		for i := 0; i < numFiles; i++ {
			wounds <- &Wound{
				Kind:  WoundKind_FILE,
				Index: int64(i),
				Start: 0,
				End:   int64(len(fakeData)),
			}
		}

		close(wounds)

		<-done
	}

	healValidate := func() {
		vc := &ValidatorContext{
			Consumer: consumer,
			HealPath: fmt.Sprintf("archive,%s", archivePath),
		}
		wtest.Must(t, vc.Validate(context.Background(), targetDir, sigInfo))
	}

	var healMethods = map[string]healMethod{
		"direct":   healDirect,
		"validate": healValidate,
	}

	assertAllFilesHealed := func() {
		for i := 0; i < numFiles; i++ {
			data, err := os.ReadFile(pathFor(i))
			assert.NoError(t, err)

			assert.Equal(t, fakeData, data)
		}
	}

	for healMethod, doHeal := range healMethods {
		wtest.Must(t, os.RemoveAll(targetDir))

		t.Logf("...with no files present (%s)", healMethod)
		doHeal()
		assertAllFilesHealed()

		t.Logf("...with one file too long (%s)", healMethod)
		assert.NoError(t, os.WriteFile(pathFor(3), bytes.Repeat(fakeData, 4), 0644))
		doHeal()
		assertAllFilesHealed()

		t.Logf("...with one file too short (%s)", healMethod)
		assert.NoError(t, os.WriteFile(pathFor(7), fakeData[:1], 0644))
		doHeal()
		assertAllFilesHealed()

		t.Logf("...with one file slightly corrupted (%s)", healMethod)
		corruptedFakeData := append([]byte{}, fakeData...)
		corruptedFakeData[2] = 255
		assert.NoError(t, os.WriteFile(pathFor(9), corruptedFakeData, 0644))
		doHeal()
		assertAllFilesHealed()
	}
}
