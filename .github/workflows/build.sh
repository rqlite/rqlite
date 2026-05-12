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
commit=`git rev-parse HEAD`
kernel=`uname -s`
buildtime=`date +%Y-%m-%dT%T%z`

# Prepare linker flags
STRIP_SYMBOLS="-w -s"
LINKER_PKG_PATH=github.com/rqlite/rqlite/v10/cmd

declare -A archs
archs=(
  ["amd64"]="gcc musl-gcc"
  ["arm64"]="aarch64-linux-gnu-gcc"
  ["arm"]="arm-linux-gnueabi-gcc arm-linux-gnueabihf-gcc"
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

    # Select GOARM for 32-bit arm builds. The hard-float (armhf) variant
    # targets ARMv7 + VFP, matching Raspberry Pi OS / Debian armhf / Ubuntu
    # armhf userlands. The soft-float (armel) variant keeps the ARMv5T
    # baseline for older devices.
    GOARM=""
    case "$compiler" in
      arm-linux-gnueabihf-gcc) GOARM=7 ;;
      arm-linux-gnueabi-gcc)   GOARM=5 ;;
    esac

    LDFLAGS="$STRIP_SYMBOLS -X $LINKER_PKG_PATH.CompilerCommand=$compiler -X $LINKER_PKG_PATH.Version=$VERSION -X $LINKER_PKG_PATH.Commit=$commit -X $LINKER_PKG_PATH.Buildtime=$buildtime"
    CGO_ENABLED=1 GOARCH=$arch GOARM=$GOARM CC=$compiler go install -a -ldflags="$LDFLAGS" ./...

    # Tarball naming. The bare "-arm" name is the hard-float build (what
    # modern ARM Linux distros expect); the soft-float build gets an
    # explicit suffix. musl keeps its existing legacy suffix.
    if [ "$compiler" == "musl-gcc" ]; then
      release=`echo rqlite-$VERSION-$kernel-$arch-musl | tr '[:upper:]' '[:lower:]'`
    elif [ "$compiler" == "arm-linux-gnueabi-gcc" ]; then
      release=`echo rqlite-$VERSION-$kernel-$arch-softfloat | tr '[:upper:]' '[:lower:]'`
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


