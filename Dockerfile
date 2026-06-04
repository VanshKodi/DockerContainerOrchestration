# ── Stage 1: Build frontend ─────────────────────────────────────────
FROM node:22-alpine AS frontend

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci
COPY frontend/ .
RUN npm run build

# ── Stage 2: Runtime ──────────────────────────────────────────────
FROM python:3.14-alpine

RUN apk add --no-cache bash

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy source code (no .venv yet — uv sync will create it below)
COPY backend/  /app/backend/
COPY proxy/    /app/proxy/
COPY frontend/ /app/frontend/

# Install Python deps
RUN cd /app/backend  && uv sync --no-dev
RUN cd /app/proxy    && uv sync --no-dev

# Overlay pre-built frontend
COPY --from=frontend /build/dist /app/frontend/dist

# Entrypoint
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

WORKDIR /app

ENV PYTHONUNBUFFERED=1

EXPOSE 20000 20001

CMD ["/app/start.sh"]
