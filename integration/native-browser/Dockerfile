# The Python image is multi-architecture and pinned to one immutable OCI index.
# Hermes itself is installed from the exact PyPI wheel published for v0.19.0;
# the URL fragment makes pip verify the release artifact before installing it.
FROM python:3.13.13-slim-bookworm@sha256:355bfa66770995d7e9a0da4b3473b44d0cb451f6b56f5615ad9c39e3c4eca03f

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      ca-certificates=20230311+deb12u1 \
      strace=6.1-0.1 \
      sudo=1.9.13p3-1+deb12u4 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 native-canary \
 && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/native-canary \
      --shell /usr/sbin/nologin native-canary \
 && chmod 0700 /home/native-canary

COPY requirements.lock /tmp/requirements.lock

RUN python -m pip install --no-cache-dir --no-deps --requirement /tmp/requirements.lock \
 && rm /tmp/requirements.lock
