#!/bin/bash

function is_semver() {
  local version_string="$1"
  local semver_pattern='^v([0-9]+)\.([0-9]+)\.([0-9]+)$'

  if [[ $version_string =~ $semver_pattern ]]; then
    return 0
  else
    return 1
  fi
}

# copy_binaries <dst_path> <src_dir>
copy_binaries () {
    cp $2/rqlited $1
    cp $2/rqlite $1
    cp $2/rqbench $1
}

# Read command line arguments
VERSION=$1
if ! is_semver "$VERSION"; then
	echo "Version $VERSION is not a valid semver version"
	exit 1
fi

# Get build parameters
machine=`uname -m`
if [ "$machine" == "x86_64" ]; then
    machine="amd64"
fi
branch=master
commit=`git rev-parse HEAD`
kernel=`uname -s`
buildtime=`date +%Y-%m-%dT%T%z`

# Prepare linker flags
STRIP_SYMBOLS="-w -s"
LINKER_PKG_PATH=github.com/rqlite/rqlite/v8/cmd
LDFLAGS="$STATIC $STRIP_SYMBOLS -X $LINKER_PKG_PATH.Version=$VERSION -X $LINKER_PKG_PATH.Branch=$branch -X $LINKER_PKG_PATH.Commit=$commit -X $LINKER_PKG_PATH.Buildtime=$buildtime"

declare -A archs
archs=(
  ["amd64"]="gcc musl-gcc"
  ["arm64"]="aarch64-linux-gnu-gcc"
  ["arm"]="arm-linux-gnueabi-gcc"
  ["riscv64"]="riscv64-linux-gnu-gcc"
  ["mips"]="mips-linux-gnu-gcc"
  ["mipsle"]="mipsel-linux-gnu-gcc"
  ["mips64"]="mips64-linux-gnuabi64-gcc"
  ["mips64le"]="mips64el-linux-gnuabi64-gcc"
  ["ppc64le"]="powerpc64le-linux-gnu-gcc"
)

for arch in "${!archs[@]}"; do
(
  compilers=${archs[$arch]}
  for compiler in $compilers; do
    echo "Building for $arch using $compiler..."

    CGO_ENABLED=1 GOARCH=$arch CC=$compiler go install -a -tags sqlite_omit_load_extension -ldflags="$LDFLAGS" ./...

    # Special case for musl-gcc, keep legacy naming.
    if [ "$compiler" == "musl-gcc" ]; then
      release=`echo rqlite-$VERSION-$kernel-$arch-musl | tr '[:upper:]' '[:lower:]'`
    else
      release=`echo rqlite-$VERSION-$kernel-$arch | tr '[:upper:]' '[:lower:]'`
    fi

    mkdir -p $release
    if [ "$arch" == "amd64" ]; then
      copy_binaries $release /home/runner/go/bin
    else
      copy_binaries $release /home/runner/go/bin/linux_$arch
    fi

    tarball=${release}.tar.gz
    tar cfz $tarball $release
    if [ $? -ne 0 ]; then
	  echo "Failed to create $tarball"
	  exit 1
    fi
  done
)
done


