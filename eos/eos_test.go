package eos_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert"

	"github.com/itchio/httpfile"
	"github.com/itchio/wharf/eos"
)

func init() {
	debugPort := os.Getenv("EOS_DEBUG_PORT")

	if debugPort != "" {
		go func() {
			log.Println(http.ListenAndServe("localhost:"+debugPort, nil))
		}()
	}
}

func Test_OpenLocalFile(t *testing.T) {
	f, err := eos.Open("/dev/null")
	assert.Nil(t, err)

	s, err := f.Stat()
	assert.Nil(t, err)

	assert.Equal(t, int64(0), s.Size())
	assert.Nil(t, f.Close())
}

type itchfs struct {
	url string
}

func (ifs *itchfs) Scheme() string {
	return "itchfs"
}

func (ifs *itchfs) MakeResource(u *url.URL) (httpfile.GetURLFunc, httpfile.NeedsRenewalFunc, error) {
	return ifs.GetURL, ifs.NeedsRenewal, nil
}

func (ifs *itchfs) GetURL() (string, error) {
	return ifs.url, nil
}

func (ifs *itchfs) NeedsRenewal(req *http.Request) bool {
	return false
}

func Test_OpenRemoteDownloadBuild(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, time.Duration(0))
	defer storageServer.CloseClientConnections()

	ifs := &itchfs{storageServer.URL}
	eos.RegisterHandler(ifs)
	defer eos.DeregisterHandler(ifs)

	f, err := eos.Open("itchfs:///upload/187770/download/builds/6996?api_key=foo")
	assert.Nil(t, err)

	s, err := f.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	readFakeData, err := ioutil.ReadAll(f)
	assert.Nil(t, err)
	assert.Equal(t, len(fakeData), len(readFakeData))
	assert.Equal(t, fakeData, readFakeData)

	readBytes, err := f.ReadAt(readFakeData, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(fakeData), readBytes)
	assert.Equal(t, fakeData, readFakeData)

	err = f.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}
}

func Test_HttpFile(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, time.Duration(0))
	defer storageServer.CloseClientConnections()

	f, err := eos.Open(storageServer.URL)
	assert.Nil(t, err)

	s, err := f.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	buf := make([]byte, 4)
	readBytes, err := f.ReadAt(buf, 4)
	assert.Nil(t, err)
	assert.Equal(t, len(buf), readBytes)
	assert.Equal(t, []byte("bbbb"), buf)

	err = f.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}
}

var _bigFakeData []byte

func getBigFakeData() []byte {
	if _bigFakeData == nil {
		for i := 1; i < 5; i++ {
			_bigFakeData = append(_bigFakeData, bytes.Repeat([]byte{byte(i * 10)}, 1*1024*1024)...)
		}
	}

	return _bigFakeData
}

func Test_HttpFileSequentialReads(t *testing.T) {
	fakeData := getBigFakeData()

	storageServer := fakeStorage(t, fakeData, time.Duration(0))
	defer storageServer.CloseClientConnections()

	f, err := eos.Open(storageServer.URL)
	assert.Nil(t, err)

	hf, ok := f.(*httpfile.HTTPFile)
	assert.True(t, ok)

	hf.ReaderStaleThreshold = time.Millisecond * time.Duration(100)

	readBuf := make([]byte, 256)
	offset := int64(0)
	readIndex := 0

	sequentialReadStop := int64(len(readBuf) * 10)

	for offset < sequentialReadStop {
		readIndex++

		if readIndex%4 == 0 {
			offset += int64(len(readBuf))
			continue
		}

		readBytes, err := f.ReadAt(readBuf, offset)
		assert.Nil(t, err)
		assert.Equal(t, len(readBuf), readBytes)

		offset += int64(readBytes)
	}

	assert.Equal(t, 1, hf.NumReaders())

	// forcing to provision a new reader
	readBytes, err := f.ReadAt(readBuf, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, 2, hf.NumReaders())

	// re-using the first one
	readBytes, err = f.ReadAt(readBuf, sequentialReadStop+int64(len(readBuf)))
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, 2, hf.NumReaders())

	// forcing a third one
	readBytes, err = f.ReadAt(readBuf, int64(len(fakeData))-int64(len(readBuf)))
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, 3, hf.NumReaders())

	// re-using second one
	readBytes, err = f.ReadAt(readBuf, int64(len(readBuf)))
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, 3, hf.NumReaders())

	// and again, skipping a few
	readBytes, err = f.ReadAt(readBuf, int64(len(readBuf)*3))
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, 3, hf.NumReaders())

	// wait for readers to become stale
	time.Sleep(time.Millisecond * time.Duration(200))

	// now just read something random, should be back to 1 reader
	readBytes, err = f.ReadAt(readBuf, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(readBuf), readBytes)
	assert.Equal(t, 1, hf.NumReaders())

	err = f.Close()
	assert.Nil(t, err)
}

func Test_HttpFileConcurrentReadAt(t *testing.T) {
	fakeData := []byte("abcdefghijklmnopqrstuvwxyz")

	storageServer := fakeStorage(t, fakeData, time.Duration(10)*time.Millisecond)
	defer storageServer.CloseClientConnections()

	f, err := eos.Open(storageServer.URL)
	assert.Nil(t, err)

	hf, ok := f.(*httpfile.HTTPFile)
	assert.True(t, ok)

	s, err := f.Stat()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	done := make(chan bool)
	errs := make(chan error)

	rand.Seed(0xDEADBEEF)
	for i := range rand.Perm(len(fakeData)) {
		go func(i int) {
			buf := make([]byte, 1)
			readBytes, err := f.ReadAt(buf, int64(i))
			if err != nil {
				errs <- err
				return
			}

			assert.Equal(t, readBytes, 1)
			assert.Equal(t, string(buf), string(fakeData[i:i+1]))

			done <- true
		}(i)
	}

	maxReaders := 0

	for i := 0; i < len(fakeData); i++ {
		numReaders := hf.NumReaders()
		if numReaders > maxReaders {
			maxReaders = numReaders
		}

		select {
		case err := <-errs:
			t.Fatal(err)
			t.FailNow()
		case <-done:
			// good!
		}
	}

	t.Logf("maximum number of readers: %d (total reads: %d)", maxReaders, len(fakeData))

	err = f.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}

	assert.Equal(t, 0, hf.NumReaders())
}

func fakeStorage(t *testing.T, content []byte, delay time.Duration) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("content-length", fmt.Sprintf("%d", len(content)))
			w.WriteHeader(200)
			return
		}

		if r.Method != "GET" {
			http.Error(w, "Invalid method", 400)
			return
		}

		time.Sleep(delay)

		w.Header().Set("content-type", "application/octet-stream")
		rangeHeader := r.Header.Get("Range")

		start := int64(0)
		end := int64(len(content)) - 1

		if rangeHeader == "" {
			w.WriteHeader(200)
		} else {
			// t.Logf("rangeHeader: %s", rangeHeader)

			equalTokens := strings.Split(rangeHeader, "=")
			if len(equalTokens) != 2 {
				http.Error(w, "Invalid range header", 400)
				return
			}

			dashTokens := strings.Split(equalTokens[1], "-")
			if len(dashTokens) != 2 {
				http.Error(w, "Invalid range header value", 400)
				return
			}

			var err error

			start, err = strconv.ParseInt(dashTokens[0], 10, 64)
			if err != nil {
				http.Error(w, fmt.Sprintf("Invalid range header start: %s", err.Error()), 400)
				return
			}

			if dashTokens[1] != "" {
				end, err = strconv.ParseInt(dashTokens[1], 10, 64)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid range header start: %s", err.Error()), 400)
					return
				}
			}

			contentRangeHeader := fmt.Sprintf("%d-%d")

			w.Header().Set("content-range", contentRangeHeader)
			w.WriteHeader(206)
		}

		sr := io.NewSectionReader(bytes.NewReader(content), start, end+1-start)
		_, err := io.Copy(w, sr)
		if err != nil {
			if strings.Contains(err.Error(), "broken pipe") {
				// ignore
			} else if strings.Contains(err.Error(), "protocol wrong type for socket") {
				// ignore
			} else {
				t.Logf("storage copy error: %s", err.Error())
				return
			}
		}

		return
	}))

	return server
}
