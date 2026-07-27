# The base image is pinned by digest: an unpinned base drifts this fixture's own
# dependency on a schedule nobody controls. Re-verify with the harness pin.
FROM debian:bookworm-slim@sha256:1209d8fd77def86ceb6663deef7956481cc6c14a25e1e64daec12c0ceffcc19d

RUN apt-get update \
 && apt-get install --no-install-recommends -y dbus-x11 gnome-keyring libsecret-tools \
 && rm -rf /var/lib/apt/lists/*

# The daemon's unlock password must be newline-terminated. Fed a bare end of
# input it starts and claims the secret-service bus name while never creating
# its collection, so a half-initialised service looks alive and serves nothing.
ENTRYPOINT ["dbus-run-session", "--"]
