package httpfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/go-errors/errors"
	"github.com/itchio/wharf/pwr"
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
	offset int64
	body   io.ReadCloser
	reader *bufio.Reader

	reading bool
}

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

		offset: -1,
		name:   parsedUrl.Path,
		size:   res.ContentLength,
	}
	return hf, nil
}

func (hf *HTTPFile) Stat() (os.FileInfo, error) {
	return &httpFileInfo{hf}, nil
}

func (hf *HTTPFile) Read(data []byte) (int, error) {
	if hf.reading {
		return 0, fmt.Errorf("concurrent read calls on HTTPFile are not supported!")
	}

	hf.reading = true
	defer func() {
		hf.reading = false
	}()

	hf.log("Read(%d)", len(data))

	if hf.offset == -1 {
		err := hf.seek(0)
		if err != nil {
			hf.log("Read: seek error: %s", err.Error())
			return 0, errors.Wrap(err, 1)
		}
	}

	bytesRead, err := hf.reader.Read(data)
	hf.offset += int64(bytesRead)

	return bytesRead, err
}

func (hf *HTTPFile) ReadAt(data []byte, offset int64) (int, error) {
	if hf.reading {
		return 0, fmt.Errorf("concurrent read calls on HTTPFile are not supported!")
	}

	hf.reading = true
	defer func() {
		hf.reading = false
	}()

	hf.log("ReadAt(%d, %d)", len(data), offset)

	diff := offset - hf.offset
	if hf.offset == -1 || diff < 0 || diff > maxDiscard {
		hf.log("ReadAt: seeking to %d, because diff = %d", offset, diff)
		err := hf.seek(offset)
		if err != nil {
			hf.log("ReadAt: seek error: %s", err.Error())
			return 0, errors.Wrap(err, 1)
		}

		hf.log("ReadAt: done seeking, now offset = %d", hf.offset)
	} else {
		if diff > 0 {
			// XXX: that's not int64-clean, could it be problematic?
			// Shouldn't be, since diff is at most maxDiscard at this point,
			// which fits in an int
			hf.log("ReadAt: discarding %d", diff)
			discarded, err := hf.reader.Discard(int(diff))
			if err != nil {
				return 0, errors.Wrap(err, 1)
			}

			if int64(discarded) != diff {
				err = fmt.Errorf("Tried to discard %d bytes, discarded %d", diff, discarded)
				return 0, errors.Wrap(err, 1)
			}

			hf.offset += diff
		}
	}

	totalBytesRead := 0
	bytesToRead := len(data)

	for totalBytesRead < bytesToRead {
		bytesRead, err := hf.reader.Read(data[totalBytesRead:])
		hf.offset += int64(bytesRead)
		totalBytesRead += bytesRead

		if err != nil {
			return totalBytesRead, err
		}
	}

	return totalBytesRead, nil
}

func (hf *HTTPFile) seek(offset int64) error {
	if hf.body != nil {
		err := hf.Close()
		if err != nil {
			return errors.Wrap(err, 1)
		}
	}

	urlStr, err := hf.getURL()
	if err != nil {
		return errors.Wrap(err, 1)
	}

	req, err := http.NewRequest("GET", urlStr, nil)
	if err != nil {
		return errors.Wrap(err, 1)
	}

	byteRange := fmt.Sprintf("bytes=%d-", offset)
	req.Header.Set("Range", byteRange)

	res, err := hf.client.Do(req)
	if err != nil {
		return errors.Wrap(err, 1)
	}
	hf.log("did request, status %d", res.StatusCode)

	if res.StatusCode == 200 && offset > 0 {
		err = fmt.Errorf("HTTP Range header not supported by %s, bailing out", req.Host)
		return errors.Wrap(err, 1)
	}

	if res.StatusCode/100 != 2 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			body = []byte("could not read error body")
			err = nil
		}

		err = fmt.Errorf("HTTP %d returned by %s (%s), bailing out", res.StatusCode, req.Host, string(body))
		return errors.Wrap(err, 1)
	}

	hf.body = res.Body
	hf.reader = bufio.NewReaderSize(hf.body, int(maxDiscard))
	hf.offset = offset
	hf.log("seek successful, now at %d", hf.offset)

	return nil
}

func (hf *HTTPFile) Close() error {
	if hf.body != nil {
		err := hf.body.Close()
		if err != nil {
			return errors.Wrap(err, 1)
		}

		hf.body = nil
		hf.reader = nil
		hf.offset = -1
	}

	return nil
}

func (hf *HTTPFile) log(format string, args ...interface{}) {
	if hf.Consumer == nil {
		return
	}

	hf.Consumer.Infof(format, args...)
}
