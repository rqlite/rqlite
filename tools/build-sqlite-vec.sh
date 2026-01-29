#!/bin/sh
set -e

# Build script for sqlite-vec extension
# Parameters:
#   $1 - Output directory for the zip file

OUTPUT_DIR="${1:-/extensions}"

log() {
    echo "[sqlite-vec] $1"
}

log "Starting sqlite-vec build"
log "Output directory: $OUTPUT_DIR"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

log "[1/5] Fetch latest release metadata from GitHub"
sqlitevec_url=$(curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | jq -r .tarball_url)
log "Downloading sqlite-vec from: $sqlitevec_url"

log "[2/5] Download tarball"
curl --retry 3 --retry-delay 2 -L "$sqlitevec_url" -o sqlite-vec.tar.gz

log "[3/5] Extract tarball"
tar xvfz sqlite-vec.tar.gz

log "[4/5] Vendor dependencies and build"
echo location >> ~/.curlrc
cd asg017*
sh scripts/vendor.sh
# Fix for missing sys/types.h include
echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c
make loadable

log "[5/5] Package dist/vec0.so into $OUTPUT_DIR/sqlite-vec.zip"
zip -j "$OUTPUT_DIR/sqlite-vec.zip" dist/vec0.so

log "Successfully built sqlite-vec extension"
