# Secret Service fixture for the credential-residence tier. The base is pinned by
# digest: an unpinned base drifts the test's own dependency on a schedule nobody
# controls.
FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      dbus \
      gnome-keyring \
      libsecret-tools \
 && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY roundtrip.sh /usr/local/bin/roundtrip.sh

RUN chmod 0755 /usr/local/bin/entrypoint.sh /usr/local/bin/roundtrip.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
