#!/bin/bash
#
# Simple script for creating releases.
#
# To determine the GitHub release ID, execute this command:
#
#   curl https://api.github.com/repos/rqlite/rqlite/releases
#
# To install musl tools run:
#
#     sudo apt-get -y install musl-dev musl-tools
#
# To install ARM and ARM64 tools run:
#
#     sudo apt-get -y install gcc make gcc-arm-linux-gnueabi binutils-arm-linux-gnueabi
#     sudo apt-get -y install gcc make gcc-aarch64-linux-gnu binutils-aarch64-linux-gnu

REPO_URL="https://github.com/rqlite/rqlite"

# copy_binaries <dst_path> <src_dir>
copy_binaries () {
	cp $2/rqlited $1
	cp $2/rqlite $1
	cp $2/rqbench $1
}

# upload_asset <path> <release ID> <API token>
upload_asset () {
	release_pkg_name=`basename $1`
	upload_url="https://uploads.github.com/repos/rqlite/rqlite/releases/$2/assets"
	curl -v -H "Content-type: application/octet-stream" -H "Authorization: token $3" -XPOST $upload_url?name=$release_pkg_name --data-binary @$1
}

if [ $# -lt 1 ]; then
    echo "$0 <version> [release_id api_token]"
    echo "Example: $0 v6.9.2 4284284 w40987joiudfigouuysdfgu_d"
    exit 1
fi

VERSION=$1
RELEASE_ID=$2
API_TOKEN=$3

# Create work directories
tmp_build=`mktemp -d`
tmp_pkg=`mktemp -d`
tmp_musl_pkg=`mktemp -d`
tmp_linux_arm64_pkg=`mktemp -d`
tmp_linux_arm_pkg=`mktemp -d`
echo "$tmp_build created for build process."
echo "$tmp_pkg created for packaging process."
echo "$tmp_musl_pkg created for musl packaging process."
echo "$tmp_linux_arm64_pkg created for Linux ARM64 packaging process."
echo "$tmp_linux_arm_pkg created for Linux ARM packaging process."

# Get common build parameters
kernel=`uname -s`
machine=`uname -m`
if [ "$machine" == "x86_64" ]; then
    machine="amd64"
fi
branch=`git rev-parse --abbrev-ref HEAD`
commit=`git rev-parse HEAD`
kernel=`uname -s`
buildtime=`date +%Y-%m-%dT%T%z`

# Prepare common linker flags
STRIP_SYMBOLS="-s"
LINKER_PKG_PATH=github.com/rqlite/rqlite/cmd
LDFLAGS="-$STRIP_SYMBOLS -X $LINKER_PKG_PATH.Version=$VERSION -X $LINKER_PKG_PATH.Branch=$branch -X $LINKER_PKG_PATH.Commit=$commit -X $LINKER_PKG_PATH.Buildtime=$buildtime"

# Prepare the source code
mkdir -p $tmp_build/src/github.com/rqlite
export GOPATH=$tmp_build
cd $tmp_build/src/github.com/rqlite
git clone $REPO_URL
cd rqlite
go get -d ./...

# Build release for this machine
rm -f $GOPATH/bin/*
if [ "$kernel" = "Linux" ]; then
	STATIC="-extldflags=-static"
fi
CGO_ENABLED=1 go install -a -tags osusergo,netgo,sqlite_omit_load_extension -ldflags="$STATIC $LDFLAGS" ./...
if [ "$kernel" = "Linux" ]; then
	ldd $GOPATH/bin/rqlited >/dev/null 2>&1
	if [ $? -ne 1 ]; then
		echo "Failed to confirm fully static linking on Linux"
		exit 1
	fi
fi

################################################################################
# Package the release native for this machine
release=`echo rqlite-$VERSION-$kernel-$machine | tr '[:upper:]' '[:lower:]'`
tarball=${release}.tar.gz
mkdir $tmp_pkg/$release
copy_binaries $tmp_pkg/$release $GOPATH/bin
( cd $tmp_pkg/; tar cvfz $tarball $release )

# Upload if passed an API token
if [ -n "$API_TOKEN" ]; then
    upload_asset $tmp_pkg/$tarball $RELEASE_ID $API_TOKEN
fi

if [ "$kernel" != "Linux" ]; then
	# Only build other versions when on Linux.
	exit 0
fi

################################################################################
# Build version for Docker use
rm -f $GOPATH/bin/*
cd $tmp_build/src/github.com/rqlite/rqlite
CGO_ENABLED=1 CC=musl-gcc go install -a -tags sqlite_omit_load_extension -ldflags="$LDFLAGS" ./...

# Package the musl release
release=`echo rqlite-$VERSION-$kernel-$machine-musl | tr '[:upper:]' '[:lower:]'`
tarball=${release}.tar.gz
mkdir $tmp_musl_pkg/$release
copy_binaries $tmp_musl_pkg/$release $GOPATH/bin
( cd $tmp_musl_pkg; tar cvfz $tarball $release )

if [ -n "$API_TOKEN" ]; then
    upload_asset $tmp_musl_pkg/$tarball $RELEASE_ID $API_TOKEN
 fi

################################################################################
# Build version for ARM64
rm -f $GOPATH/bin/*
cd $tmp_build/src/github.com/rqlite/rqlite
CGO_ENABLED=1 GOARCH=arm64 CC=aarch64-linux-gnu-gcc go install -a -tags sqlite_omit_load_extension -ldflags="$LDFLAGS" ./...

# Package the ARM64 release
release=`echo rqlite-$VERSION-$kernel-arm64 | tr '[:upper:]' '[:lower:]'`
tarball=${release}.tar.gz
mkdir $tmp_linux_arm64_pkg/$release
copy_binaries $tmp_linux_arm64_pkg/$release $GOPATH/bin/linux_arm64
( cd $tmp_linux_arm64_pkg; tar cvfz $tarball $release )

if [ -n "$API_TOKEN" ]; then
    upload_asset $tmp_linux_arm64_pkg/$tarball $RELEASE_ID $API_TOKEN
fi

################################################################################
# Build version for ARM32
rm -f $GOPATH/bin/*
cd $tmp_build/src/github.com/rqlite/rqlite
CGO_ENABLED=1 GOARCH=arm CC=arm-linux-gnueabi-gcc go install -a -tags sqlite_omit_load_extension -ldflags="$LDFLAGS" ./...

# Package the ARM32 release
release=`echo rqlite-$VERSION-$kernel-arm | tr '[:upper:]' '[:lower:]'`
tarball=${release}.tar.gz
mkdir $tmp_linux_arm_pkg/$release
copy_binaries $tmp_linux_arm_pkg/$release $GOPATH/bin/linux_arm
( cd $tmp_linux_arm_pkg; tar cvfz $tarball $release )

if [ -n "$API_TOKEN" ]; then
    upload_asset $tmp_linux_arm_pkg/$tarball $RELEASE_ID $API_TOKEN
fi

