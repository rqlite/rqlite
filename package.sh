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

echo "$tmp_build created for build process."
echo "$tmp_pkg created for packaging process."

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

sudo apt-get -y install musl-dev musl-tools

LINKER_PKG_PATH=github.com/rqlite/rqlite/cmd
CGO_ENABLED=1 CC=musl-gcc go install -a -installsuffix cgo -ldflags="-X $LINKER_PKG_PATH.Version=$VERSION -X $LINKER_PKG_PATH.Branch=$branch -X $LINKER_PKG_PATH.Commit=$commit -X $LINKER_PKG_PATH.Buildtime=$buildtime" ./...

if [ `uname` = "Linux" ]; then
	ldd $GOPATH/bin/rqlited
	if [ $? -ne 1 ]; then
		echo "Failed to confirm fully static linking on Linux"
		exit 1
	fi
fi

release=`echo rqlite-$VERSION-$kernel-$machine | tr '[:upper:]' '[:lower:]'`
release_pkg=${release}.tar.gz
mkdir $tmp_pkg/$release
cp $GOPATH/bin/rqlited $tmp_pkg/$release
cp $GOPATH/bin/rqlite $tmp_pkg/$release
cp $GOPATH/bin/rqbench $tmp_pkg/$release
cd $tmp_pkg
tar cvfz $release_pkg $release

if [ -z "$API_TOKEN" ]; then
    exit 0
fi

upload_url="https://uploads.github.com/repos/rqlite/rqlite/releases/$RELEASE_ID/assets"
curl -v -H "Content-type: application/octet-stream" -H "Authorization: token $API_TOKEN" -XPOST $upload_url?name=$release_pkg --data-binary @$release_pkg
