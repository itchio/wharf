package option

import (
	"net/http"

	itchio "github.com/itchio/go-itchio"
)

type EOSSettings struct {
	ItchClient *itchio.Client
	HTTPClient *http.Client
}

func DefaultSettings() *EOSSettings {
	return &EOSSettings{
		HTTPClient: http.DefaultClient,
	}
}

//////////////////////////////////////

type Option interface {
	Apply(*EOSSettings)
}

//////////////////////////////////////

type itchClientOption struct {
	itchClient *itchio.Client
}

var _ Option = (*itchClientOption)(nil)

func (ico *itchClientOption) Apply(s *EOSSettings) {
	s.ItchClient = ico.itchClient
}

func WithItchClient(itchClient *itchio.Client) Option {
	return &itchClientOption{itchClient}
}
