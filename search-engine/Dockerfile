# syntax=docker/dockerfile:1
#
# Search Engine Core: the internal gRPC SearchService on :50051 (`search-engine`), and the
# Silver -> OpenSearch indexer (`search-indexer`, same image, another command).
# Build context: the repository root (needs search-engine/ and the shared database/
# package).
#
# The code runs from its source layout (/app is search-engine/): it finds
# models/lightgbm_reranker.txt relative to its own files, exactly as
# `PYTHONPATH=search-engine/src python -m pgs_search.grpc.server` does locally. So only
# its dependencies are installed; the project is put on PYTHONPATH, not pip-installed.

ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim-trixie AS build
# CPU-only PyTorch: the code never moves a model to a GPU, and the default PyPI wheel
# pulls in several GB of CUDA libraries.
ARG TORCH_VERSION=2.14.1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"
COPY search-engine/pyproject.toml search-engine/constraints.txt /src/search-engine/
COPY database/constraints.txt /src/database/constraints.txt
# The heavy, rarely changing layer: the dependency list read from
# search-engine/pyproject.toml, every version in its closure pinned by
# search-engine/constraints.txt (plus database/constraints.txt).
RUN --mount=type=cache,target=/root/.cache/pip \
    python -c "import tomllib; deps = tomllib.load(open('/src/search-engine/pyproject.toml', 'rb'))['project']['dependencies']; print('\n'.join(deps))" \
        > /tmp/search-engine-requirements.txt \
    && pip install \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        "torch==${TORCH_VERSION}+cpu" \
        --requirement /tmp/search-engine-requirements.txt \
        --constraint /src/search-engine/constraints.txt \
        --constraint /src/database/constraints.txt
# The shared pgs-db package changes often; on its own layer it does not invalidate the one
# above.
COPY database/pyproject.toml database/README.md /src/database/
COPY database/src /src/database/src
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install \
        --constraint /src/search-engine/constraints.txt \
        --constraint /src/database/constraints.txt \
        "/src/database[postgres]"

FROM python:${PYTHON_VERSION}-slim-trixie
# LightGBM's wheel links against the system OpenMP runtime.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    HF_HOME=/home/app/.cache/huggingface
# The Hugging Face models (NLLB-200 ~2.5 GB, LaBSE ~1.8 GB, both at pinned revisions)
# download on first start into HF_HOME, which docker-compose.yml backs with a volume.
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home --home-dir /home/app \
        --shell /usr/sbin/nologin app \
    && mkdir -p "${HF_HOME}" \
    && chown -R app:app /home/app/.cache
COPY --from=build /opt/venv /opt/venv

WORKDIR /app
COPY search-engine/ ./

USER 10001:10001
EXPOSE 50051
# The server binds its port only after creating the index and preloading every model, so
# an open port means ready (it also serves grpc.health.v1). The long start period covers
# the first start's model download.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30m --start-interval=15s --retries=3 \
    CMD ["python", "-c", "import socket; socket.create_connection(('127.0.0.1', 50051), timeout=3).close()"]
CMD ["python", "-m", "pgs_search.grpc.server"]
