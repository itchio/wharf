package uploader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/dustin/go-humanize"
	"github.com/itchio/go-itchio"
	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/pwr"
	"github.com/itchio/wharf/splitfunc"
)

var seed = 0

// ResumableUpload keeps track of an upload and reports back on its progress
type ResumableUpload struct {
	c *itchio.Client

	TotalBytes    int64
	UploadedBytes int64
	OnProgress    func()

	// resumable URL as per GCS
	uploadURL string

	// where data is written so we can update counts
	writeCounter io.Writer

	// need to flush to squeeze all the data out
	bufferedWriter *bufio.Writer

	// need to close so reader end of pipe gets EOF
	pipeWriter io.Closer

	id       int
	consumer *pwr.StateConsumer
}

// Close flushes all intermediary buffers and closes the connection
func (ru *ResumableUpload) Close() error {
	var err error

	ru.Debugf("flushing buffered writer, %d written", ru.TotalBytes)

	err = ru.bufferedWriter.Flush()
	if err != nil {
		return err
	}

	ru.Debugf("closing pipe writer")

	err = ru.pipeWriter.Close()
	if err != nil {
		return err
	}

	ru.Debugf("closed pipe writer")
	ru.Debugf("everything closed! uploadedbytes = %d, totalbytes = %d", ru.UploadedBytes, ru.TotalBytes)

	return nil
}

// Write is our implementation of io.Writer
func (ru *ResumableUpload) Write(p []byte) (int, error) {
	return ru.writeCounter.Write(p)
}

func NewResumableUpload(uploadURL string, done chan bool, errs chan error, consumer *pwr.StateConsumer) (*ResumableUpload, error) {
	ru := &ResumableUpload{}
	ru.uploadURL = uploadURL
	ru.id = seed
	seed++
	ru.consumer = consumer
	ru.c = itchio.ClientWithKey("x")

	pipeR, pipeW := io.Pipe()

	ru.pipeWriter = pipeW

	// TODO: make configurable?
	const bufferSize = 32 * 1024 * 1024

	bufferedWriter := bufio.NewWriterSize(pipeW, bufferSize)
	ru.bufferedWriter = bufferedWriter

	onWrite := func(count int64) {
		ru.Debugf("onwrite %d", count)
		ru.TotalBytes = count
		if ru.OnProgress != nil {
			ru.OnProgress()
		}
	}
	ru.writeCounter = counter.NewWriterCallback(onWrite, bufferedWriter)

	onRead := func(count int64) {
		ru.Debugf("onread %d", count)
		ru.UploadedBytes = count
		if ru.OnProgress != nil {
			ru.OnProgress()
		}
	}
	readCounter := counter.NewReaderCallback(onRead, pipeR)

	go ru.uploadChunks(readCounter, done, errs)

	return ru, nil
}

func (ru *ResumableUpload) Debugf(f string, args ...interface{}) {
	ru.consumer.Debugf("[upload %d] %s", ru.id, fmt.Sprintf(f, args...))
}

const minBlockSize = 256 * 1024 // 256KB

func (ru *ResumableUpload) uploadChunks(reader io.Reader, done chan bool, errs chan error) {
	var offset int64 = 0

	s := bufio.NewScanner(reader)
	s.Buffer(make([]byte, minBlockSize), 0)
	s.Split(splitfunc.New(minBlockSize))

	sentEnd := false

	sendBytes := func(buf []byte) error {
		buflen := int64(len(buf))
		ru.Debugf("received %d bytes", buflen)

		body := bytes.NewReader(buf)
		req, err := http.NewRequest("PUT", ru.uploadURL, body)
		if err != nil {
			return err
		}

		start := offset
		end := start + buflen - 1
		contentRange := fmt.Sprintf("bytes %d-%d/*", offset, end)

		if buflen < minBlockSize {
			contentRange = fmt.Sprintf("bytes %d-%d/%d", offset, end, offset+buflen)
			sentEnd = true
		}

		req.Header.Set("content-range", contentRange)

		res, err := ru.c.Do(req)
		if err != nil {
			return err
		}

		if res.StatusCode != 200 && res.StatusCode != 308 {
			ru.Debugf("uh oh, got HTTP %s", res.Status)
			resb, _ := ioutil.ReadAll(res.Body)
			ru.Debugf("server said %s", string(resb))
			return fmt.Errorf("HTTP %d while uploading", res.StatusCode)
		}

		offset += buflen
		ru.Debugf("%s uploaded, at %s", humanize.Bytes(uint64(offset)), res.Status)
		return nil
	}

	for s.Scan() {
		err := sendBytes(s.Bytes())
		if err != nil {
			errs <- err
			return
		}
	}

	err := s.Err()
	if err != nil {
		ru.Debugf("scanner error :(")
		errs <- err
		return
	}

	if !sentEnd {
		ru.Debugf("sending empty packet b/c we're doing a round number", offset)
		sendBytes([]byte{})
	}

	done <- true
	ru.Debugf("done sent!")
}
