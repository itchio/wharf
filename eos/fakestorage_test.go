package eos

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

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

			contentRangeHeader := fmt.Sprintf("%d-%d", start, end)
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
