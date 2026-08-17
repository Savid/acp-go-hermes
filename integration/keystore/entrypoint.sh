#!/bin/sh
# Brings up a session bus and an unlocked Secret Service, then parks.
set -eu

FIXTURE_DIR=/run/acp-go-hermes-keystore
UNLOCK_PASSWORD=canary-unlock

mkdir -p "$FIXTURE_DIR" "$HOME/.local/share/keyrings"

DBUS_SESSION_BUS_ADDRESS=$(dbus-daemon --session --fork --print-address=1)
export DBUS_SESSION_BUS_ADDRESS

# The unlock password is newline-terminated on purpose. Fed a bare end of input
# the daemon starts and claims the secret-service bus name while never creating
# its collection, so a half-initialised service looks alive and serves nothing.
printf '%s\n' "$UNLOCK_PASSWORD" | gnome-keyring-daemon --unlock --components=secrets >/dev/null

printf 'export DBUS_SESSION_BUS_ADDRESS=%s\n' "$DBUS_SESSION_BUS_ADDRESS" >"$FIXTURE_DIR/env"

# Readiness is the store/lookup round trip in roundtrip.sh, run against this
# service; this marker only tells the matrix it is inside the fixture.
touch "$FIXTURE_DIR/marker"

exec sleep infinity
