#!/bin/bash
#
# Build .deb and .rpm packages for rqlite (amd64 and arm64).
# Reuses binaries already produced by build.sh.
#
# Usage: package/build-packages.sh <VERSION>
#   VERSION must be a semver tag, e.g. v9.4.2

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---------------------------------------------------------------------------
# Validate version argument
# ---------------------------------------------------------------------------
VERSION="${1:-}"
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Usage: $0 <VERSION>  (e.g. v9.4.2)"
    exit 1
fi

# Strip leading 'v' for package version strings (e.g. 9.4.2)
PKG_VERSION="${VERSION#v}"

# Architecture mapping: go-arch -> deb-arch rpm-arch binary-dir-suffix
declare -A DEB_ARCH=( ["amd64"]="amd64"   ["arm64"]="arm64" )
declare -A RPM_ARCH=( ["amd64"]="x86_64"  ["arm64"]="aarch64" )

BINARIES="rqlited rqlite rqbench"

# ---------------------------------------------------------------------------
# Locate binary directory produced by build.sh
#   amd64 -> rqlite-<version>-linux-amd64/
#   arm64 -> rqlite-<version>-linux-arm64/
# ---------------------------------------------------------------------------
bin_dir_for() {
    local arch="$1"
    echo "rqlite-${VERSION}-linux-${arch}"
}

# ---------------------------------------------------------------------------
# Build .deb package for a given Go architecture
# ---------------------------------------------------------------------------
build_deb() {
    local arch="$1"
    local deb_arch="${DEB_ARCH[$arch]}"
    local src_dir
    src_dir="$(bin_dir_for "$arch")"

    if [[ ! -d "$src_dir" ]]; then
        echo "WARN: binary directory $src_dir not found, skipping .deb for $arch"
        return
    fi

    local staging="rqlite-deb-${deb_arch}"
    rm -rf "$staging"

    # Create directory tree
    mkdir -p "$staging/DEBIAN"
    mkdir -p "$staging/usr/bin"
    mkdir -p "$staging/lib/systemd/system"
    mkdir -p "$staging/var/lib/rqlite"

    # Control file
    cat > "$staging/DEBIAN/control" <<EOF
Package: rqlite
Version: ${PKG_VERSION}
Architecture: ${deb_arch}
Maintainer: rqlite <rqlite@rqlite.io>
Depends: libc6
Homepage: https://rqlite.io
Description: rqlite - lightweight, distributed relational database
 rqlite is a lightweight, user-friendly, distributed relational database
 built on SQLite with Raft consensus.
EOF

    # Post-install and pre-remove scripts
    cp "$SCRIPT_DIR/scripts/postinstall.sh" "$staging/DEBIAN/postinst"
    chmod 0755 "$staging/DEBIAN/postinst"
    cp "$SCRIPT_DIR/scripts/preremove.sh" "$staging/DEBIAN/prerm"
    chmod 0755 "$staging/DEBIAN/prerm"

    # Binaries
    for bin in $BINARIES; do
        cp "$src_dir/$bin" "$staging/usr/bin/$bin"
        chmod 0755 "$staging/usr/bin/$bin"
    done

    # Systemd unit
    cp "$SCRIPT_DIR/rqlited.service" "$staging/lib/systemd/system/rqlited.service"
    chmod 0644 "$staging/lib/systemd/system/rqlited.service"

    # Build
    local deb_name="rqlite_${PKG_VERSION}_${deb_arch}.deb"
    dpkg-deb --build --root-owner-group "$staging" "$deb_name"
    echo "Built $deb_name"

    rm -rf "$staging"
}

# ---------------------------------------------------------------------------
# Build .rpm package for a given Go architecture
# ---------------------------------------------------------------------------
build_rpm() {
    local arch="$1"
    local rpm_arch="${RPM_ARCH[$arch]}"
    local src_dir
    src_dir="$(bin_dir_for "$arch")"

    if [[ ! -d "$src_dir" ]]; then
        echo "WARN: binary directory $src_dir not found, skipping .rpm for $arch"
        return
    fi

    local topdir
    topdir="$(pwd)/rpmbuild-${rpm_arch}"
    rm -rf "$topdir"
    mkdir -p "$topdir"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

    # Absolute path to source binaries
    local abs_src_dir
    abs_src_dir="$(cd "$src_dir" && pwd)"
    local abs_script_dir
    abs_script_dir="$(cd "$SCRIPT_DIR" && pwd)"

    # Generate spec file
    cat > "$topdir/SPECS/rqlite.spec" <<EOF
Name:           rqlite
Version:        ${PKG_VERSION}
Release:        1
Summary:        rqlite - lightweight, distributed relational database
License:        MIT
URL:            https://rqlite.io
Requires:       glibc

%description
rqlite is a lightweight, user-friendly, distributed relational database
built on SQLite with Raft consensus.

%install
mkdir -p %{buildroot}/usr/bin
mkdir -p %{buildroot}/lib/systemd/system
mkdir -p %{buildroot}/var/lib/rqlite

cp ${abs_src_dir}/rqlited   %{buildroot}/usr/bin/rqlited
cp ${abs_src_dir}/rqlite    %{buildroot}/usr/bin/rqlite
cp ${abs_src_dir}/rqbench   %{buildroot}/usr/bin/rqbench
chmod 0755 %{buildroot}/usr/bin/rqlited
chmod 0755 %{buildroot}/usr/bin/rqlite
chmod 0755 %{buildroot}/usr/bin/rqbench

cp ${abs_script_dir}/rqlited.service %{buildroot}/lib/systemd/system/rqlited.service
chmod 0644 %{buildroot}/lib/systemd/system/rqlited.service

%files
/usr/bin/rqlited
/usr/bin/rqlite
/usr/bin/rqbench
/lib/systemd/system/rqlited.service
%dir /var/lib/rqlite

%post
if ! getent group rqlite >/dev/null 2>&1; then
    groupadd --system rqlite
fi
if ! getent passwd rqlite >/dev/null 2>&1; then
    useradd --system --gid rqlite --home-dir /var/lib/rqlite --shell /usr/sbin/nologin --no-create-home rqlite
fi
mkdir -p /var/lib/rqlite
chown rqlite:rqlite /var/lib/rqlite
chmod 750 /var/lib/rqlite
systemctl daemon-reload

%preun
if systemctl is-active --quiet rqlited 2>/dev/null; then
    systemctl stop rqlited
fi
if systemctl is-enabled --quiet rqlited 2>/dev/null; then
    systemctl disable rqlited
fi
EOF

    rpmbuild -bb --target "$rpm_arch" --define "_topdir $topdir" "$topdir/SPECS/rqlite.spec"

    # Copy RPM to working directory
    local rpm_file
    rpm_file=$(find "$topdir/RPMS" -name "*.rpm" | head -1)
    if [[ -n "$rpm_file" ]]; then
        cp "$rpm_file" .
        echo "Built $(basename "$rpm_file")"
    else
        echo "ERROR: rpm build produced no output for $arch"
        return 1
    fi

    rm -rf "$topdir"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "Building packages for rqlite ${VERSION} ..."

for arch in amd64 arm64; do
    build_deb "$arch"
    build_rpm "$arch"
done

echo ""
echo "Package build complete. Output files:"
ls -1 rqlite*.deb rqlite*.rpm 2>/dev/null || echo "(none found)"
