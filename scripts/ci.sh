#!/bin/sh

go version

export CURRENT_BUILD_PATH=$(pwd)
export GOPATH=$CURRENT_BUILD_PATH
export PKG=github.com/itchio/wharf

mkdir -p src/$PKG
rsync -a --exclude 'src' . src/$PKG
go get -v -d -t $PKG/...
go get -v $PKG/...

go list -f '{{if gt (len .TestGoFiles) 0}}"go test -covermode count -coverprofile {{.Name}}.coverprofile -coverpkg $PKG/... {{.ImportPath}}"{{end}}' $PKG/... | xargs -I {} bash -c {}

go get -v github.com/wadey/gocovmerge

gocovmerge `ls *.coverprofile` > coverage.txt
go tool cover -func=coverage.txt
