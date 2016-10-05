#!/bin/sh

go version

export CURRENT_BUILD_PATH=$(pwd)
export GOPATH=$CURRENT_BUILD_PATH
export PKG=github.com/itchio/wharf

mkdir -p src/$PKG
rsync -a --exclude 'src' . src/$PKG
go get -v -d -t $PKG/...
go test -v -cover $PKG/...
