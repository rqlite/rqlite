#!/bin/bash
#
# Simple script for creating releases.

if [ $# -lt 1 ]; then
    echo "$0 <version> [api_token release_id]"
    exit 1
fi

REPO_URL="https://github.com/otoolep/rqlite"

VERSION=$1
API_TOKEN=$2
RELEASE_ID=$3

tmp_build=`mktemp -d`
tmp_pkg=`mktemp -d`

kernel=`uname -s`
machine=`uname -m`
if [ "$machine" == "x86_64" ]; then
    machine="amd64"
fi
branch=`git rev-parse --abbrev-ref HEAD`
commit=`git rev-parse HEAD`
kernel=`uname -s`

mkdir -p $tmp_build/src/github.com/otoolep
export GOPATH=$tmp_build
cd $tmp_build/src/github.com/otoolep
git clone $REPO_URL
cd rqlite
go get -d ./...
go install -ldflags="-X main.version=$VERSION -X main.branch=$branch -X main.commit=$commit" ./...

release=`echo rqlited-$VERSION-$kernel-$machine | tr '[:upper:]' '[:lower:]'`
release_pkg=${release}.tar.gz
mkdir $tmp_pkg/$release
cp $GOPATH/bin/rqlited $tmp_pkg/$release
cp $GOPATH/bin/rqlite $tmp_pkg/$release
cd $tmp_pkg
tar cvfz $release_pkg $release

if [ -z "$API_TOKEN" ]; then
    exit 0
fi

upload_url="https://uploads.github.com/repos/otoolep/rqlite/releases/$RELEASE_ID/assets"
curl -v -H "Context-type: application/octet-stream" -H "Authorization: token $API_TOKEN" -XPOST $upload_url?name=$release_pkg --data-binary @$release_pkg
