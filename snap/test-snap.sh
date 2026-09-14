#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
MANIFEST="$ROOT/snap/snapcraft.yaml"
WRAPPER="$ROOT/snap/command-rqlited"

for required in \
    'name: rqlite' \
    'confinement: strict' \
    'command: bin/command-rqlited' \
    'command: bin/rqlite' \
    'command: bin/rqbench'; do
    if ! grep -Fq "$required" "$MANIFEST"; then
        printf 'missing snap metadata: %s\n' "$required" >&2
        exit 1
    fi
done

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
mkdir -p "$tmpdir/snap/bin"
cat > "$tmpdir/snap/bin/rqlited" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$SNAP/args"
EOF
chmod 0755 "$tmpdir/snap/bin/rqlited"

SNAP="$tmpdir/snap" SNAP_COMMON="$tmpdir/common" \
    sh "$WRAPPER" -http-addr=127.0.0.1:4001

expected="$tmpdir/common/data"
test -d "$expected"
test "$(sed -n '1p' "$tmpdir/snap/args")" = '-http-addr=127.0.0.1:4001'
test "$(sed -n '2p' "$tmpdir/snap/args")" = "$expected"
