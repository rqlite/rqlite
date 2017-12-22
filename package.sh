#!/bin/bash
#
# Simple script for creating releases.
#
# To determine the GitHub release ID, execute this command:
#
#   curl https://api.github.com/repos/rqlite/rqlite/releases
#

if [ $# -lt 1 ]; then
    echo "$0 <version> [release_id api_token]"
    exit 1
fi

REPO_URL="https://github.com/rqlite/rqlite"

VERSION=$1
RELEASE_ID=$2
API_TOKEN=$3

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
buildtime=`date +%Y-%m-%dT%T%z`

mkdir -p $tmp_build/src/github.com/rqlite
export GOPATH=$tmp_build
cd $tmp_build/src/github.com/rqlite
git clone $REPO_URL
cd rqlite
go get -d ./...
go install -ldflags="-X main.version=$VERSION -X main.branch=$branch -X main.commit=$commit -X main.buildtime=$buildtime" ./...

release=`echo rqlite-$VERSION-$kernel-$machine | tr '[:upper:]' '[:lower:]'`
release_pkg=${release}.tar.gz
mkdir $tmp_pkg/$release
cp $GOPATH/bin/rqlited $tmp_pkg/$release
cp $GOPATH/bin/rqlite $tmp_pkg/$release
cd $tmp_pkg
tar cvfz $release_pkg $release

if [ -z "$API_TOKEN" ]; then
    exit 0
fi

upload_url="https://uploads.github.com/repos/rqlite/rqlite/releases/$RELEASE_ID/assets"
curl -v -H "Content-type: application/octet-stream" -H "Authorization: token $API_TOKEN" -XPOST $upload_url?name=$release_pkg --data-binary @$release_pkg
