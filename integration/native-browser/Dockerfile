# Build the exact official Hermes v0.20.0 source revision used by the canary.
# The source archive, uv image, Python image, and official Python lock are
# immutable inputs. PyPI does not publish a Hermes 0.20 wheel.
FROM ghcr.io/astral-sh/uv:0.12.3-python3.13-trixie@sha256:8e231456b9e5eab17151e10ca096b8a3d52936be045d6a48e006bede5f229a9a AS hermes_source
ARG HERMES_COMMIT=3c27eb6234bf91b8ceee9e9071591b31e9b148cb

ENV UV_PYTHON_INSTALL_DIR=/opt/python
WORKDIR /opt/hermes
COPY source.sha256 /tmp/hermes-source.sha256
RUN curl -fsSL --retry 3 \
      -o /tmp/hermes-agent.tar.gz \
      "https://codeload.github.com/NousResearch/hermes-agent/tar.gz/${HERMES_COMMIT}" \
 && sha256sum -c /tmp/hermes-source.sha256 \
 && tar -xzf /tmp/hermes-agent.tar.gz --strip-components=1 -C /opt/hermes
RUN uv python install --no-cache --no-bin 3.11.15 \
 && PYTHON_BIN="$(uv python find --managed-python 3.11.15)" \
 && UV_PYTHON_DOWNLOADS=never uv sync --locked --no-dev --no-cache --python "$PYTHON_BIN" \
 && test "$(.venv/bin/python --version)" = "Python 3.11.15" \
 && test "$(.venv/bin/python -c 'import sqlite3; print(sqlite3.sqlite_version)')" = "3.53.1" \
 && .venv/bin/hermes --version | grep -Fx 'Hermes Agent v0.20.0 (2026.8.3)'

FROM python:3.11.15-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3 AS runtime_tools

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      ca-certificates=20230311+deb12u1 \
      strace=6.1-0.1 \
 && rm -rf /var/lib/apt/lists/*

FROM ghcr.io/astral-sh/uv:0.12.3-python3.13-trixie@sha256:8e231456b9e5eab17151e10ca096b8a3d52936be045d6a48e006bede5f229a9a

COPY --from=runtime_tools /usr/bin/strace /usr/bin/strace
COPY --from=hermes_source /opt/hermes /opt/hermes
COPY --from=hermes_source /opt/python /opt/python

ENV HERMES_DISABLE_LAZY_INSTALLS=1
ENV UV_PYTHON_DOWNLOADS=never
WORKDIR /opt/hermes
RUN test "$(.venv/bin/python --version)" = "Python 3.11.15" \
 && test "$(.venv/bin/python -c 'import sqlite3; print(sqlite3.sqlite_version)')" = "3.53.1" \
 && .venv/bin/hermes --version | grep -Fx 'Hermes Agent v0.20.0 (2026.8.3)' \
 && ln -s /opt/hermes/.venv/bin/hermes /usr/local/bin/hermes
