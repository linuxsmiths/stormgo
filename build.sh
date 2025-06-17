#!/bin/bash

# Exit on error.
set -e

echo "Using $(go version)"

BUILD_DIR="$(dirname $(realpath $0))/build"
mkdir -p $BUILD_DIR 2>/dev/null

build()
{
    if [ -z "$1" ]; then
        echo "Please provide a directory to build."
        exit 1
    fi

    echo "Building $1..."
    pushd $1 > /dev/null 2>&1

    # Enforce proper formatting. Fail build if gofmt reports errors.
    fmtdiff=$(gofmt -d .)
    if [ -n "$fmtdiff" ]; then
        echo
        echo "*** go format errors ***"
        echo "Please run \"gofmt -d .\" inside $1 folder and make sure it doesn't print anything!"
        echo
        echo ">>>>> snip <<<<<"
        echo "$fmtdiff"
        echo ">>>>> snip <<<<<"
        exit 1
    fi

    GOARCH=amd64 GOOS=linux go build -o $BUILD_DIR/$1
    echo "$BUILD_DIR/$1 ready!"

    popd > /dev/null 2>&1
}

go fmt ./...
build "terminal"
