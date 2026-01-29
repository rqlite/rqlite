#!/bin/sh
set -e

# Build script for rqlite-sqlite-ext extensions (misc and icu)
# Parameters:
#   $1 - Output directory for the zip files

OUTPUT_DIR="${1:-/extensions}"

log() {
    echo "[rqlite-ext] $1"
}

log "Starting rqlite-sqlite-ext build"
log "Output directory: $OUTPUT_DIR"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

log "[1/4] Clone rqlite-sqlite-ext repository"
git clone https://github.com/rqlite/rqlite-sqlite-ext.git

log "[2/4] Build misc extension"
cd rqlite-sqlite-ext/misc
make
zip "$OUTPUT_DIR/misc.zip" *.so
cd ../..

log "[3/4] Build icu extension"
cd rqlite-sqlite-ext/icu
gcc -fPIC -shared icu.c -I .. `pkg-config --libs --cflags icu-uc icu-io` -o icu.so

log "[4/4] Package icu.so into $OUTPUT_DIR/icu.zip"
zip "$OUTPUT_DIR/icu.zip" icu.so
cd ../..

log "Successfully built rqlite-sqlite-ext extensions (misc and icu)"
