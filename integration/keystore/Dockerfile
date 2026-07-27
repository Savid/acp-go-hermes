# The base image is pinned by digest: an unpinned base drifts this fixture's own
# dependency on a schedule nobody controls. Re-verify with the harness pin.
FROM debian:bookworm-slim@sha256:1209d8fd77def86ceb6663deef7956481cc6c14a25e1e64daec12c0ceffcc19d

RUN apt-get update \
 && apt-get install --no-install-recommends -y dbus gnome-keyring libsecret-tools \
 && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY roundtrip.sh /usr/local/bin/roundtrip.sh

RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/roundtrip.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
