# Build the exact official Hermes v0.21.1 source revision used by the canary.
# The source archive, uv image, Python image, and official Python lock are
# immutable inputs. PyPI does not publish a Hermes 0.21 wheel.
FROM ghcr.io/astral-sh/uv:0.12.3-python3.13-trixie@sha256:8e231456b9e5eab17151e10ca096b8a3d52936be045d6a48e006bede5f229a9a AS hermes_source
ARG HERMES_COMMIT=2237be355906fbe6065ce1815711eee52b2d646e

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
 && .venv/bin/hermes --version | grep -Fx 'Hermes Agent v0.21.1 (2026.9.7)'

FROM ghcr.io/astral-sh/uv:0.12.3-python3.13-trixie@sha256:8e231456b9e5eab17151e10ca096b8a3d52936be045d6a48e006bede5f229a9a

RUN apt-get update \
 && apt-get install --no-install-recommends --assume-yes \
      strace=6.13+ds-1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=hermes_source /opt/hermes /opt/hermes
COPY --from=hermes_source /opt/python /opt/python

ENV HERMES_DISABLE_LAZY_INSTALLS=1
ENV UV_PYTHON_DOWNLOADS=never
WORKDIR /opt/hermes
RUN test "$(.venv/bin/python --version)" = "Python 3.11.15" \
 && test "$(.venv/bin/python -c 'import sqlite3; print(sqlite3.sqlite_version)')" = "3.53.1" \
 && .venv/bin/hermes --version | grep -Fx 'Hermes Agent v0.21.1 (2026.9.7)' \
 && ln -s /opt/hermes/.venv/bin/hermes /usr/local/bin/hermes
