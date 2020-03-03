#!/bin/sh -xe

go version

export PATH="$PATH:$(go env GOPATH)/bin"

PACKAGES=$(go list -f "{{ .Dir }}" ./... | sed 's/[\]/\//g' | sed 's/.*wharf[/]//')

for PKG in ${PACKAGES}; do
    go test -v -race ./$PKG
done

go test -cover -coverprofile=coverage.txt ./...

curl -s https://codecov.io/bash | bash
