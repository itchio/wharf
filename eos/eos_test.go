package eos_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/alecthomas/assert"
	itchio "github.com/itchio/go-itchio"
	"github.com/itchio/wharf/eos"
	"github.com/itchio/wharf/eos/option"
)

func Test_Open(t *testing.T) {
	f, err := eos.Open("/dev/null")
	assert.Nil(t, err)

	s, err := f.Stat()
	assert.Nil(t, err)

	assert.Equal(t, int64(0), s.Size())
	assert.Nil(t, f.Close())
}

func Test_OpenRemoteDownloadBuild(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData)
	defer storageServer.Close()

	t.Logf("storage server URL: %s", storageServer.URL)

	apiServer, client := fakeAPI(200, `{
		"archive": {
			"url": "`+storageServer.URL+`"
		}
	}`)
	defer apiServer.Close()

	f, err := eos.Open("itchfs:///upload/187770/download/builds/6996?api_key=foo", option.WithItchClient(client))
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

func fakeAPI(code int, body string) (*httptest.Server, *itchio.Client) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		fmt.Fprintln(w, body)
	}))

	// Make a transport that reroutes all traffic to the example server
	transport := &http.Transport{
		Proxy: func(req *http.Request) (*url.URL, error) {
			return url.Parse(server.URL)
		},
	}

	// Make a http.Client with the transport
	httpClient := &http.Client{Transport: transport}

	client := &itchio.Client{
		Key:        "APIKEY",
		HTTPClient: httpClient,
		BaseURL:    server.URL,
	}

	return server, client
}

func fakeStorage(t *testing.T, content []byte) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.Header().Set("content-length", fmt.Sprintf("%d", len(content)))
			w.WriteHeader(200)
			return
		}

		if r.Method != "GET" {

		}

		w.Header().Set("content-type", "application/octet-stream")
		rangeHeader := r.Header.Get("Range")

		start := int64(0)
		end := int64(len(content))

		if rangeHeader == "" {
			w.WriteHeader(200)
		} else {
			t.Logf("rangeHeader: %s", rangeHeader)

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

		sr := io.NewSectionReader(bytes.NewReader(content), start, end-start+1)
		_, err := io.Copy(w, sr)
		if err != nil {
			t.Fatalf("storage copy error: %s", err.Error())
			t.FailNow()
			return
		}

		return
	}))

	return server
}
