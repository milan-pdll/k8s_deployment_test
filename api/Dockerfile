# syntax=docker/dockerfile:1
#
# API gateway: FastAPI served by Uvicorn on :8000.
# Build context: the repository root -- besides api/ the image needs the shared
# database/ package and the search engine's generated gRPC stubs.

ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim-trixie AS build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
COPY api/requirements.txt /src/api/requirements.txt
COPY database/constraints.txt database/pyproject.toml database/README.md /src/database/
COPY database/src /src/database/src
# The `auth` extra (argon2-cffi) is what pgs_db.security needs to check admin passwords.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install \
        --requirement /src/api/requirements.txt \
        --constraint /src/database/constraints.txt \
        "/src/database[postgres,auth]"

FROM python:${PYTHON_VERSION}-slim-trixie
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /opt/venv /opt/venv

WORKDIR /app
# The application package only (api/src/pgs_api; not its tests or docs).
COPY api/src/pgs_api/ ./pgs_api/
# pgs_api/grpc/client.py imports pgs_search.grpc.generated. Only those protocol stubs are
# copied (with their empty parent __init__ files): the search engine itself, with its
# ML stack, runs in its own container and is reached over gRPC.
COPY search-engine/src/pgs_search/__init__.py ./pgs_search/
COPY search-engine/src/pgs_search/grpc/__init__.py ./pgs_search/grpc/
COPY search-engine/src/pgs_search/grpc/generated/ ./pgs_search/grpc/generated/

USER 10001:10001
EXPOSE 8000
# Liveness only: /health/live answers without touching PostgreSQL or the search engine
# (GET /health/ready is the dependency-aware probe).
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=3)"]
# Request logging is the app's own (JSON, with request ids, without query strings).
CMD ["uvicorn", "pgs_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
