FROM python:3.11-slim

RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (layer caching), skip building the project itself
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source
COPY voz_crawler/ ./voz_crawler/

# Install the project package
RUN uv sync --frozen --no-dev

ENV DAGSTER_HOME=/dagster_home
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

# Bake dagster config so run containers launched by DockerRunLauncher can find it.
# (dagster_webserver/daemon override this via bind mount; user_code + run containers use this copy)
RUN mkdir -p /dagster_home
COPY docker/dev/dagster.docker.yaml /dagster_home/dagster.yaml

EXPOSE 4000
CMD ["uv", "run", "dagster", "code-server", "start", \
     "--host", "0.0.0.0", \
     "--port", "4000", \
     "--module-name", "voz_crawler.definitions"]
