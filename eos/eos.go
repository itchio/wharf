// eos stands for 'enhanced os', it mostly supplies 'eos.Open', which supports
// the 'itchfs://' scheme to access remote files
package eos

import (
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/go-errors/errors"
	itchio "github.com/itchio/go-itchio"
	"github.com/itchio/wharf/eos/httpfile"
	"github.com/itchio/wharf/eos/option"
)

type File interface {
	io.Reader
	io.Closer
	io.ReaderAt

	Stat() (os.FileInfo, error)
}

func Open(name string, opts ...option.Option) (File, error) {
	settings := option.DefaultSettings()

	for _, opt := range opts {
		opt.Apply(settings)
	}

	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "":
		return os.Open(name)
	case "http", "https":
		getURL := func() (string, error) {
			return name, nil
		}
		return httpfile.New(getURL, settings.HTTPClient)
	case "itchfs":
		getURL, err := makeItchUrlGetter(u, settings)
		if err != nil {
			return nil, err
		}

		hf, err := httpfile.New(getURL, settings.HTTPClient)
		if err != nil {
			return nil, err
		}

		return hf, nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}

func makeItchUrlGetter(u *url.URL, settings *option.EOSSettings) (httpfile.GetURLFunc, error) {
	vals := u.Query()

	apiKey := vals.Get("api_key")
	if apiKey == "" {
		return nil, fmt.Errorf("missing API key")
	}

	var itchClient *itchio.Client

	if settings.ItchClient != nil {
		itchClient = settings.ItchClient
		itchClient.Key = apiKey
	} else {
		itchClient = itchio.ClientWithKey(apiKey)
	}

	if u.Host != "" {
		return nil, fmt.Errorf("invalid itchfs URL (must start with itchfs:///): %s", u.String())
	}

	source, err := ObtainSource(itchClient, u.Path)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}

	return source.makeGetURL()
}
