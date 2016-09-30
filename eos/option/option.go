package option

import "net/http"

type EOSSettings struct {
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
