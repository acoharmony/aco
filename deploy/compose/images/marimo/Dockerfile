# Use uv base image with Python 3.13
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco

# Install git (required by hatch-vcs for dynamic versioning during uv run)
RUN apt-get update && apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Create care group and user with matching IDs from host
RUN groupadd -g 1002 care && \
    groupadd -g 989 docker && \
    useradd -m -u 1002 -g 1002 -G 989 -s /bin/bash care

# Create necessary directories with correct permissions
RUN mkdir -p /home/care/.config/marimo /home/care/.cache/uv && \
    chown -R 1002:1002 /home/care

# Copy marimo config
COPY conf/marimo/marimo.toml /home/care/.config/marimo/marimo.toml
RUN chown 1002:1002 /home/care/.config/marimo/marimo.toml

# Set environment to use mounted venv from host
ENV UV_PROJECT_ENVIRONMENT=/home/care/acoharmony/.venv
# Ensure Python 3.13 from uv is first, then venv
ENV PATH="/home/care/.local/share/uv/python/cpython-3.13.7-linux-x86_64-gnu/bin:/home/care/acoharmony/.venv/bin:$PATH"
ENV HOME=/home/care

# Copy entrypoint script (injects custom homepage into marimo's index.html at startup)
COPY images/marimo/entrypoint.sh /usr/local/bin/entrypoint.sh

# Switch to care user
USER care

# Set working directory to notebooks
WORKDIR /opt/s3/data/notebooks

# Expose marimo default port
EXPOSE 10012

ENTRYPOINT ["entrypoint.sh"]
