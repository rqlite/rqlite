#!/bin/sh
set -e

# Stop rqlited service if running
if systemctl is-active --quiet rqlited 2>/dev/null; then
    systemctl stop rqlited
fi

# Disable rqlited service if enabled
if systemctl is-enabled --quiet rqlited 2>/dev/null; then
    systemctl disable rqlited
fi
