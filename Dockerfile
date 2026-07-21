# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.11
ARG UV_VERSION=0.11.16

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv-bin

FROM node:20-bookworm-slim AS pm2-stage
ARG PM2_VERSION=5.4.3
RUN npm install -g "pm2@${PM2_VERSION}" \
    && npm cache clean --force

FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

COPY --from=uv-bin /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project


FROM python:${PYTHON_VERSION}-slim-bookworm AS production

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
        libgomp1 \
        libpq5 \
        procps \
        proj-data \
        tini \
    && rm -rf /var/lib/apt/lists/* /tmp/* /root/.cache

COPY --from=pm2-stage /usr/local/bin/node /usr/local/bin/node
COPY --from=pm2-stage /usr/local/bin/pm2 /usr/local/bin/pm2
COPY --from=pm2-stage /usr/local/bin/pm2-runtime /usr/local/bin/pm2-runtime
COPY --from=pm2-stage /usr/local/bin/pm2-dev /usr/local/bin/pm2-dev
COPY --from=pm2-stage /usr/local/lib/node_modules/pm2 /usr/local/lib/node_modules/pm2

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
CMD ["uv", "run", "streamlit", "run", "apps/streamlit/Geovisor.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.maxUploadSize=300", "--server.maxMessageSize=300"]
