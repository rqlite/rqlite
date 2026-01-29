#!/bin/sh
set -euo pipefail

# Build script for sqliteai-vector extension
# Parameters:
#   $1 - Output directory for the zip file
#   $2 - Target OS (optional, defaults to unset)
#   $3 - Target architecture (optional, defaults to unset)

OUTPUT_DIR="${1:-/extensions}"
TARGETOS="${2:-}"
TARGETARCH="${3:-}"

log() {
    echo "[sqliteai] $1"
}

log "Starting sqliteai-vector build"
log "Output directory: $OUTPUT_DIR"
log "TARGETOS=${TARGETOS:-<unset>} TARGETARCH=${TARGETARCH:-<unset>}"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

log "[1/7] Fetch latest release metadata from GitHub"
meta_json="$(mktemp)"
if ! curl --retry 3 --retry-delay 2 -fsSL "https://api.github.com/repos/sqliteai/sqlite-vector/releases/latest" -o "$meta_json"; then
    log "FAIL [1/7] curl GitHub API (releases/latest)"
    exit 1
fi

log "[2/7] Extract tarball_url via jq"
sqliteai_vector="$(jq -er '.tarball_url' "$meta_json")" || {
    log "FAIL [2/7] jq parse .tarball_url (missing/invalid JSON)"
    log "---- begin GitHub API response ----"
    head -c 2000 "$meta_json" || true
    echo
    log "---- end GitHub API response ----"
    exit 1
}
rm -f "$meta_json"
log "tarball_url=$sqliteai_vector"

log "[3/7] Download tarball"
if ! curl -fL --retry 3 --retry-delay 2 "$sqliteai_vector" -o sqliteai-vector.tar.gz; then
    log "FAIL [3/7] curl tarball"
    exit 1
fi

log "[4/7] Extract tarball"
if ! tar xvfz sqliteai-vector.tar.gz; then
    log "FAIL [4/7] tar extract"
    exit 1
fi
rm -f sqliteai-vector.tar.gz

log "[5/7] Enter extracted directory"
dir="$(ls -d sqliteai* 2>/dev/null | head -n 1)"
if [ -z "$dir" ]; then
    log "FAIL [5/7] could not find extracted directory matching 'sqliteai*'"
    ls -la
    exit 1
fi
cd "$dir"

log "[6/7] Build (make)"
if ! make; then
    log "FAIL [6/7] make"
    exit 1
fi

log "[7/7] Package dist/vector.so into $OUTPUT_DIR/sqliteai-vector.zip"
if [ ! -f dist/vector.so ]; then
    log "FAIL [7/7] expected output file dist/vector.so not found"
    ls -la dist || true
    exit 1
fi
if ! zip -j "$OUTPUT_DIR/sqliteai-vector.zip" dist/vector.so; then
    log "FAIL [7/7] zip"
    exit 1
fi

log "Successfully built sqliteai-vector extension"
