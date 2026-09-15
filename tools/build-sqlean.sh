#!/bin/sh
set -e

# Build script for sqlean extension
# Parameters:
#   $1 - Output directory for the zip file

OUTPUT_DIR="${1:-/extensions}"

log() {
    echo "[sqlean] $1"
}

log "Starting sqlean build"
log "Output directory: $OUTPUT_DIR"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

log "[1/4] Fetch latest release metadata from GitHub"
sqlean_url=$(curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | jq -r .tarball_url)
log "Downloading sqlean from: $sqlean_url"

log "[2/4] Download tarball"
curl --retry 5 --retry-delay 5 -L "$sqlean_url" -o sqlean.tar.gz

log "[3/4] Extract and build"
tar xvfz sqlean.tar.gz
cd nalgeon*
make prepare-dist download-sqlite download-external compile-linux

log "[4/4] Package dist/sqlean.so into $OUTPUT_DIR/sqlean.zip"
zip -j "$OUTPUT_DIR/sqlean.zip" dist/sqlean.so

log "Successfully built sqlean extension"
