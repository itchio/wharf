package httpfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pwr"
	uuid "github.com/satori/go.uuid"
)

// A GetURLFunc returns a URL we can download the resource from.
// It's handy to have this as a function rather than a constant for signed expiring URLs
type GetURLFunc func() (urlString string, err error)

// amount we're willing to download and throw away
const maxDiscard int64 = 1 * 1024 * 1024 // 1MB

var ErrNotFound = errors.New("HTTP file not found on server")

type HTTPFile struct {
	getURL GetURLFunc
	client *http.Client

	Consumer *pwr.StateConsumer

	name   string
	size   int64
	offset int64 // for io.ReadSeeker

	ReaderStaleThreshold time.Duration

	closed bool

	readers      map[string]*httpReader
	readersMutex sync.Mutex
}

type httpReader struct {
	file      *HTTPFile
	id        string
	touchedAt time.Time
	offset    int64
	body      io.ReadCloser
	reader    *bufio.Reader
}

const DefaultReaderStaleThreshold = time.Second * time.Duration(10)

func (hr *httpReader) Stale() bool {
	return time.Since(hr.touchedAt) > hr.file.ReaderStaleThreshold
}

func (hr *httpReader) Read(data []byte) (int, error) {
	hr.touchedAt = time.Now()
	readBytes, err := hr.reader.Read(data)
	hr.offset += int64(readBytes)
	return readBytes, err
}

func (hr *httpReader) Discard(n int) (int, error) {
	hr.touchedAt = time.Now()
	discarded, err := hr.reader.Discard(n)
	hr.offset += int64(discarded)
	return discarded, err
}

func (hr *httpReader) Close() error {
	return hr.body.Close()
}

var _ io.Seeker = (*HTTPFile)(nil)
var _ io.Reader = (*HTTPFile)(nil)
var _ io.ReaderAt = (*HTTPFile)(nil)
var _ io.Closer = (*HTTPFile)(nil)

func New(getURL GetURLFunc, client *http.Client) (*HTTPFile, error) {
	urlStr, err := getURL()
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	parsedUrl, err := url.Parse(urlStr)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	req, err := http.NewRequest("HEAD", urlStr, nil)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return nil, errors.Wrap(ErrNotFound, 1)
		}

		err = fmt.Errorf("Expected HTTP 200, got HTTP %d for %s", res.StatusCode, urlStr)
		return nil, errors.Wrap(err, 1)
	}

	hf := &HTTPFile{
		getURL: getURL,
		client: client,

		name:    parsedUrl.Path,
		size:    res.ContentLength,
		readers: make(map[string]*httpReader),

		ReaderStaleThreshold: DefaultReaderStaleThreshold,
	}
	return hf, nil
}

func (hf *HTTPFile) NumReaders() int {
	return len(hf.readers)
}

func (hf *HTTPFile) borrowReader(offset int64) (*httpReader, error) {
	hf.readersMutex.Lock()
	defer hf.readersMutex.Unlock()

	var bestReader string
	var bestDiff int64 = math.MaxInt64

	for _, reader := range hf.readers {
		if reader.Stale() {
			delete(hf.readers, reader.id)

			err := reader.Close()
			if err != nil {
				return nil, err
			}
			continue
		}

		diff := offset - reader.offset
		if diff >= 0 && diff < maxDiscard {
			if diff < bestDiff {
				bestReader = reader.id
				bestDiff = diff
			}
		}
	}

	if bestReader != "" {
		// re-use!
		reader := hf.readers[bestReader]
		delete(hf.readers, bestReader)

		// discard if needed
		if bestDiff > 0 {
			hf.log("borrow: for %d, re-using %d by discarding %d bytes", offset, reader.offset, bestDiff)

			// XXX: not int64-clean
			_, err := reader.Discard(int(bestDiff))
			if err != nil {
				return nil, err
			}
		}

		return reader, nil
	}

	hf.log("borrow: making fresh for offset %d", offset)

	// provision a new one
	urlStr, err := hf.getURL()
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	byteRange := fmt.Sprintf("bytes=%d-", offset)
	req.Header.Set("Range", byteRange)

	res, err := hf.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}
	hf.log("did request, status %d", res.StatusCode)

	if res.StatusCode == 200 && offset > 0 {
		err = fmt.Errorf("HTTP Range header not supported by %s, bailing out", req.Host)
		return nil, errors.Wrap(err, 1)
	}

	if res.StatusCode/100 != 2 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			body = []byte("could not read error body")
			err = nil
		}

		err = fmt.Errorf("HTTP %d returned by %s (%s), bailing out", res.StatusCode, req.Host, string(body))
		return nil, errors.Wrap(err, 1)
	}

	reader := &httpReader{
		file:      hf,
		id:        uuid.NewV4().String(),
		touchedAt: time.Now(),
		offset:    offset,
		reader:    bufio.NewReaderSize(res.Body, int(maxDiscard)),
		body:      res.Body,
	}

	return reader, nil
}

func (hf *HTTPFile) returnReader(reader *httpReader) {
	hf.readersMutex.Lock()
	defer hf.readersMutex.Unlock()

	// TODO: enforce max idle readers ?

	reader.touchedAt = time.Now()
	hf.readers[reader.id] = reader
}

func (hf *HTTPFile) Stat() (os.FileInfo, error) {
	return &httpFileInfo{hf}, nil
}

func (hf *HTTPFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_END:
		newOffset = hf.size + offset
	case os.SEEK_CUR:
		newOffset = hf.offset + offset
	default:
		return hf.offset, fmt.Errorf("invalid whence value %d", whence)
	}

	if newOffset < 0 {
		newOffset = 0
	}

	if newOffset > hf.size {
		newOffset = hf.size
	}

	hf.offset = newOffset
	return hf.offset, nil
}

func (hf *HTTPFile) Read(data []byte) (int, error) {
	hf.log("Read(%d)", len(data))

	reader, err := hf.borrowReader(hf.offset)
	if err != nil {
		return 0, err
	}

	defer hf.returnReader(reader)

	bytesRead, err := reader.Read(data)
	hf.offset += int64(bytesRead)

	return bytesRead, err
}

func (hf *HTTPFile) ReadAt(data []byte, offset int64) (int, error) {
	hf.log("ReadAt(%d, %d)", len(data), offset)

	reader, err := hf.borrowReader(offset)
	if err != nil {
		return 0, err
	}

	defer hf.returnReader(reader)

	totalBytesRead := 0
	bytesToRead := len(data)

	for totalBytesRead < bytesToRead {
		bytesRead, err := reader.Read(data[totalBytesRead:])
		hf.offset += int64(bytesRead)
		totalBytesRead += bytesRead

		if err != nil {
			return totalBytesRead, err
		}
	}

	return totalBytesRead, nil
}

func (hf *HTTPFile) Close() error {
	if hf.closed {
		return nil
	}

	hf.readersMutex.Lock()
	defer hf.readersMutex.Unlock()

	for id, reader := range hf.readers {
		err := reader.Close()
		if err != nil {
			return errors.Wrap(err, 1)
		}

		delete(hf.readers, id)
	}

	hf.closed = true

	return nil
}

func (hf *HTTPFile) log(format string, args ...interface{}) {
	if hf.Consumer == nil {
		return
	}

	hf.Consumer.Infof(format, args...)
}
