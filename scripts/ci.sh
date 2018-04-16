#!/bin/sh -xe

go version

export GOPATH=$PWD/gopath
rm -rf $GOPATH

export PKG=github.com/itchio/wharf
export PATH=$PATH:$GOPATH/bin

mkdir -p $GOPATH/src/$PKG
rsync -a --exclude 'gopath' . $GOPATH/src/$PKG

go get -v -d -t $PKG/...
go test -v -cover -coverprofile=coverage.txt -race $PKG/...

curl -s https://codecov.io/bash | bash
