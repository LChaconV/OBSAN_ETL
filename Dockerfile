# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.11
ARG UV_VERSION=0.11.16

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv-bin

FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

COPY --from=uv-bin /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        g++ \
        gcc \
        libgdal-dev \
        libgeos-dev \
        libhdf5-dev \
        libnetcdf-dev \
        libpq-dev \
        libproj-dev \
        pkg-config \
        proj-data \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project


FROM python:${PYTHON_VERSION}-slim-bookworm AS production

ARG PM2_VERSION=5.4.3

COPY --from=uv-bin /uv /uvx /usr/local/bin/

ENV APP_HOME=/app \
    DEBIAN_FRONTEND=noninteractive \
    HOME=/tmp \
    PATH="/app/.venv/bin:/usr/local/bin:${PATH}" \
    PM2_HOME=/tmp/.pm2 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    UV_CACHE_DIR=/tmp/uv-cache \
    UV_NO_SYNC=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gdal-bin \
        libexpat1 \
        libgdal32 \
        libgeos-c1v5 \
        libgomp1 \
        libhdf5-103-1 \
        libjpeg62-turbo \
        libnetcdf19 \
        libpng16-16 \
        libpq5 \
        libproj25 \
        libsqlite3-0 \
        libtiff6 \
        nodejs \
        npm \
        proj-data \
        tini \
    && npm install -g "pm2@${PM2_VERSION}" \
    && npm cache clean --force \
    && apt-get purge -y --auto-remove npm \
    && rm -rf /var/lib/apt/lists/* /tmp/* /root/.cache

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home --home-dir /tmp/app --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app . .

RUN python - <<'PY'
from pathlib import Path

import streamlit

index_path = Path(streamlit.__file__).parent / "static" / "index.html"
html = index_path.read_text(encoding="utf-8")
marker = "    <script>\n      window.prerenderReady = false\n    </script>"
backend_config = """    <script>
      window.__streamlit = {
        ...(window.__streamlit || {}),
        BACKEND_BASE_URL: window.location.origin,
        HOST_CONFIG_BASE_URL: window.location.origin,
      }
    </script>
"""

if backend_config not in html:
    if marker not in html:
        raise RuntimeError(f"No se pudo parchear {index_path}: marker no encontrado")
    html = html.replace(marker, f"{backend_config}{marker}", 1)
    index_path.write_text(html, encoding="utf-8")
PY

RUN mkdir -p data/bronze data/silver data/golden logs state \
    && rm -f config/state_db.yaml \
    && ln -s ../state/state_db.yaml config/state_db.yaml \
    && chown -R app:app data logs state \
    && chown -h app:app config/state_db.yaml

USER 10001:10001

EXPOSE 8501

ENTRYPOINT ["tini", "--"]
CMD ["uv", "run", "streamlit", "run", "apps/streamlit/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.maxUploadSize=500", "--server.maxMessageSize=500"]
