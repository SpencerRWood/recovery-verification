FROM ghcr.io/astral-sh/uv:0.10.0 AS uv

FROM python:3.14-slim AS builder
COPY --from=uv /uv /uvx /usr/local/bin/
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable --no-install-project && \
    uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim
RUN useradd --create-home --uid 10001 app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
USER app
CMD ["sh", "-c", "exec dagster api grpc -m recovery_verification.dagster.definitions -h 0.0.0.0 -p \"${DAGSTER_GRPC_PORT:?DAGSTER_GRPC_PORT is required}\""]
