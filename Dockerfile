# Use an official Python runtime as a parent image
FROM python:3.12.6-slim

# Set the working directory in the container
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    git \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install Rye non-interactively
RUN curl -sSf https://rye.astral.sh/get | RYE_INSTALL_OPTION="--yes" bash

# Set the PATH to include Rye
ENV PATH="/root/.rye/bin:/root/.rye/shims:${PATH}"

# Set RYE_VENV_PATH to ensure the virtual environment is created in /app/.venv
ENV RYE_VENV_PATH=/app/.venv

# Ensure the /app directory is writable
RUN mkdir -p /app && chmod -R 777 /app

# Copy project files into the container
COPY . .

# Use Rye to create the virtual environment and install dependencies
RUN rye env create --force
RUN rye sync

# Copy and set permissions for the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Define the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
