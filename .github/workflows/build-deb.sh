#!/bin/bash
set -e

function is_semver() {
  local version_string="$1"
  local semver_pattern='^v([0-9]+)\.([0-9]+)\.([0-9]+)$'

  if [[ $version_string =~ $semver_pattern ]]; then
    return 0
  else
    return 1
  fi
}

# Read command line arguments
VERSION=$1
if ! is_semver "$VERSION"; then
	echo "Version $VERSION is not a valid semver version"
	exit 1
fi

# Strip 'v' prefix from version for package version
PKG_VERSION=${VERSION#v}

echo "Building Debian package for version $PKG_VERSION"

# Create package directory structure
DEB_ROOT="pkg/deb-root"
rm -rf pkg
mkdir -p "$DEB_ROOT"/{usr/bin,lib/systemd/system,etc/default,var/lib/rqlite,DEBIAN}

# Find the musl binaries (prefer musl for maximum compatibility)
MUSL_DIR=""
for dir in rqlite-${VERSION}-linux-amd64-musl; do
    if [ -d "$dir" ]; then
        MUSL_DIR="$dir"
        break
    fi
done

# If musl directory not found, fall back to regular glibc build
BIN_DIR=""
if [ -n "$MUSL_DIR" ]; then
    echo "Using musl binaries from $MUSL_DIR"
    BIN_DIR="$MUSL_DIR"
    # Update control template to remove libc6 dependency for musl build
    CONTROL_DEPENDS=""
else
    # Look for regular glibc build
    for dir in rqlite-${VERSION}-linux-amd64; do
        if [ -d "$dir" ]; then
            BIN_DIR="$dir"
            echo "Using glibc binaries from $BIN_DIR"
            CONTROL_DEPENDS="Depends: libc6 (>= 2.31)"
            break
        fi
    done
fi

if [ -z "$BIN_DIR" ]; then
    echo "Error: Could not find amd64 binaries directory"
    echo "Looking for: rqlite-${VERSION}-linux-amd64-musl or rqlite-${VERSION}-linux-amd64"
    ls -la
    exit 1
fi

# Copy binaries
echo "Copying binaries from $BIN_DIR to package..."
cp "$BIN_DIR/rqlited" "$DEB_ROOT/usr/bin/"
cp "$BIN_DIR/rqlite" "$DEB_ROOT/usr/bin/"
cp "$BIN_DIR/rqbench" "$DEB_ROOT/usr/bin/"

# Set binary permissions
chmod 755 "$DEB_ROOT/usr/bin"/{rqlited,rqlite,rqbench}

# Copy systemd service file
cp packaging/rqlite.service "$DEB_ROOT/lib/systemd/system/"

# Copy default environment file
cp packaging/rqlite.default "$DEB_ROOT/etc/default/rqlite"

# Create DEBIAN/control file
sed "s/VERSION_PLACEHOLDER/$PKG_VERSION/" packaging/control.template > "$DEB_ROOT/DEBIAN/control"

# If using glibc build, ensure depends line is present
if [ -n "$CONTROL_DEPENDS" ]; then
    echo "Setting libc6 dependency for glibc build"
else
    echo "Removing libc6 dependency for musl build"
    sed -i '/^Depends:/d' "$DEB_ROOT/DEBIAN/control"
fi

# Copy maintainer scripts
cp packaging/postinst "$DEB_ROOT/DEBIAN/"
cp packaging/prerm "$DEB_ROOT/DEBIAN/"

# Set permissions for maintainer scripts
chmod 755 "$DEB_ROOT/DEBIAN"/{postinst,prerm}

# Set directory permissions in package
chmod 755 "$DEB_ROOT/var/lib/rqlite"

# Build the package
PACKAGE_NAME="rqlite_${PKG_VERSION}_amd64.deb"
echo "Building package: $PACKAGE_NAME"

# Use fakeroot to preserve ownership and build as non-root
if command -v fakeroot >/dev/null 2>&1; then
    fakeroot dpkg-deb --build --root-owner-group "$DEB_ROOT" "$PACKAGE_NAME"
else
    dpkg-deb --build --root-owner-group "$DEB_ROOT" "$PACKAGE_NAME"
fi

if [ $? -eq 0 ]; then
    echo "Successfully built $PACKAGE_NAME"
    ls -la "$PACKAGE_NAME"
else
    echo "Failed to build package"
    exit 1
fi