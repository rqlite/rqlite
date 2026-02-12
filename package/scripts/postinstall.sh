#!/bin/sh
set -e

# Create rqlite system group if it doesn't exist
if ! getent group rqlite >/dev/null 2>&1; then
    groupadd --system rqlite
fi

# Create rqlite system user if it doesn't exist
if ! getent passwd rqlite >/dev/null 2>&1; then
    useradd --system --gid rqlite --home-dir /var/lib/rqlite --shell /usr/sbin/nologin --no-create-home rqlite
fi

# Create data directory and set ownership
mkdir -p /var/lib/rqlite
chown rqlite:rqlite /var/lib/rqlite
chmod 750 /var/lib/rqlite

# Reload systemd
systemctl daemon-reload
