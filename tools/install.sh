#!/bin/sh
# install.sh — Download and install rqlite on Linux.
#
# Usage:
#   curl -fsSL https://rqlite.io/install.sh | sh
#
# Environment variables:
#   RQLITE_VERSION   Version to install (e.g. v9.4.1). Default: latest.
#   INSTALL_DIR      Directory to install binaries into. Default: /usr/local/bin.

set -e

GITHUB_REPO="rqlite/rqlite"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

info() {
    printf '%s\n' "$@"
}

error() {
    printf 'Error: %s\n' "$@" >&2
    exit 1
}

# Check that we're running on Linux.
check_os() {
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$os" in
        linux) ;;
        darwin)
            error "macOS is not supported by this installer. Install rqlite with Homebrew instead:" \
                  "  brew install rqlite"
            ;;
        *)
            error "Unsupported operating system: $os"
            ;;
    esac
}

# Map uname -m to the Go architecture name used in release assets.
detect_arch() {
    arch=$(uname -m)
    case "$arch" in
        x86_64)      arch="amd64" ;;
        aarch64)     arch="arm64" ;;
        armv6l)      arch="arm" ;;
        armv7l)      arch="arm" ;;
        riscv64)     arch="riscv64" ;;
        mips)        arch="mips" ;;
        mipsel)      arch="mipsle" ;;
        mips64)      arch="mips64" ;;
        mips64el)    arch="mips64le" ;;
        ppc64le)     arch="ppc64le" ;;
        *)           error "Unsupported architecture: $arch" ;;
    esac
    echo "$arch"
}

# Resolve the version to install.
get_version() {
    if [ -n "$RQLITE_VERSION" ]; then
        # Ensure the version starts with 'v'.
        case "$RQLITE_VERSION" in
            v*) echo "$RQLITE_VERSION" ;;
            *)  echo "v${RQLITE_VERSION}" ;;
        esac
        return
    fi

    # Get the latest release tag via GitHub redirect.
    url=$(curl -fsSL -o /dev/null -w '%{url_effective}' \
        "https://github.com/${GITHUB_REPO}/releases/latest") \
        || error "Failed to determine latest release. Set RQLITE_VERSION and try again."
    version="${url##*/}"
    if [ -z "$version" ]; then
        error "Failed to parse latest release version from: $url"
    fi
    echo "$version"
}

# Run a command, prepending sudo if the target directory is not writable.
maybe_sudo() {
    if [ -w "$INSTALL_DIR" ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    elif command -v doas >/dev/null 2>&1; then
        doas "$@"
    else
        error "Cannot write to $INSTALL_DIR and neither sudo nor doas is available." \
              "Re-run with INSTALL_DIR set to a writable path, for example:" \
              "  INSTALL_DIR=~/.local/bin sh install.sh"
    fi
}

main() {
    check_os

    arch=$(detect_arch)
    version=$(get_version)

    info "Installing rqlite ${version} (linux/${arch}) to ${INSTALL_DIR}"

    # Build download URL.
    tarball="rqlite-${version}-linux-${arch}.tar.gz"
    url="https://github.com/${GITHUB_REPO}/releases/download/${version}/${tarball}"

    # Create a temporary directory for the download.
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT

    info "Downloading ${url}..."
    curl -fsSL "$url" -o "${tmp}/${tarball}" \
        || error "Download failed. Check that ${version} is a valid release and linux/${arch} is available."

    info "Extracting..."
    tar -xzf "${tmp}/${tarball}" -C "$tmp"

    # The archive contains a directory with the binaries.
    src="${tmp}/rqlite-${version}-linux-${arch}"

    # Install binaries.
    mkdir -p "$INSTALL_DIR"
    for bin in rqlited rqlite rqbench; do
        if [ -f "${src}/${bin}" ]; then
            maybe_sudo install -m 755 "${src}/${bin}" "${INSTALL_DIR}/${bin}"
        fi
    done

    info ""
    info "rqlite ${version} installed successfully to ${INSTALL_DIR}"
    info ""
    info "Start a node:  rqlited ~/data"
    info "Connect:       rqlite"
    info ""
    info "Documentation: https://rqlite.io/docs/"
}

main
