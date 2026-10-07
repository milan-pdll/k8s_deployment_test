# syntax=docker/dockerfile:1
#
# ETL image: Airflow (scheduling only) + the ETL processing environment (the Temporal ETL
# worker, which is the Spark driver, and the Spark standalone master/workers/executors).
# Build context: the repository root (needs ETL/ and the shared database/ package).
#
# One image runs every ETL role in the root docker-compose.yml: airflow-webserver,
# airflow-scheduler, etl-worker, spark-master and spark-worker. Spark executors need the
# same Python and packages as the driver, which is why the cluster runs this image too.
#
# Two Python environments, because Airflow 2.10 requires SQLAlchemy < 2 and pgs-db
# requires SQLAlchemy 2:
#   - Airflow's own (the image's /home/airflow/.local): Airflow plus what the DAGs import
#     (temporalio to start workflows, kafka-python to read/commit the site events).
#     DAGs never import pgs-db, PySpark or torch.
#   - /opt/etl-venv: the processing stack -- pgs-db, PySpark, LaBSE (torch CPU,
#     sentence-transformers), ClamAV and S3 clients, temporalio. etl-worker runs with
#     /opt/etl-venv/bin/python, and that interpreter is what Spark starts on the executors.

FROM apache/airflow:2.10.4-python3.11

# Build-time only (ARGs reach RUN as env vars but don't persist into the image). The
# processing environment downloads several large wheels (torch, pyspark), and pip's
# default 15 s read timeout and 5 retries fail the build on a slow or flaky link.
ARG PIP_DEFAULT_TIMEOUT=120
ARG PIP_RETRIES=10
ARG PIP_DISABLE_PIP_VERSION_CHECK=1

USER 0
# PySpark needs a JVM.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# The processing environment. Created from the base interpreter (/usr/local/bin/python),
# not Airflow's `python` (first on PATH, the /home/airflow/.local environment): running
# that one as root would write root-owned .pyc files into Airflow's site-packages. Every
# version is pinned (ETL/constraints.txt, database/constraints.txt); PyTorch is the
# CPU-only build -- nothing runs on a GPU, and the default wheel adds several GB of CUDA.
# The pip cache is a BuildKit cache mount, so a failed build reuses downloaded wheels.
COPY ETL/requirements.txt ETL/constraints.txt /tmp/etl/
COPY database/constraints.txt /tmp/etl/database-constraints.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    /usr/local/bin/python -m venv /opt/etl-venv \
    && /opt/etl-venv/bin/pip install \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        --requirement /tmp/etl/requirements.txt \
        --constraint /tmp/etl/constraints.txt \
        --constraint /tmp/etl/database-constraints.txt
# pgs-db on its own layer: it changes far more often than the stack above.
COPY database/pyproject.toml database/README.md /tmp/pgs-db/
COPY database/src /tmp/pgs-db/src
RUN --mount=type=cache,target=/root/.cache/pip \
    /opt/etl-venv/bin/pip install \
        --constraint /tmp/etl/constraints.txt \
        --constraint /tmp/etl/database-constraints.txt \
        "/tmp/pgs-db[postgres]" \
    && rm -rf /tmp/pgs-db /tmp/etl

# Hugging Face cache for the LaBSE model (a volume in docker-compose), writable by any
# AIRFLOW_UID: group 0 like the rest of the Airflow image.
ENV HF_HOME=/home/airflow/.cache/huggingface
RUN mkdir -p "${HF_HOME}" \
    && chown airflow:0 "${HF_HOME}" \
    && chmod 2775 "${HF_HOME}"

USER 50000
# Airflow's environment: only what the DAGs import. apache-airflow is named in the same
# command so pip can never up- or downgrade it.
RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000,gid=0 \
    pip install \
        "apache-airflow==${AIRFLOW_VERSION}" \
        temporalio==1.34.0 \
        kafka-python==2.2.15

# ETL code, at the paths the DAGs and compose use (/opt/airflow/spark_lib for the
# pipeline modules). docker-compose.yml bind-mounts the same directories over these for
# live editing; the copies make the image runnable on its own (and on Kubernetes).
COPY ETL/airflow/dags /opt/airflow/dags
COPY ETL/airflow/plugins /opt/airflow/plugins
COPY ETL/airflow/config /opt/airflow/config
COPY ETL/spark /opt/airflow/spark_lib
COPY ETL/temporal /opt/etl/temporal

# Spark's launcher scripts (spark-class, used by spark-master/-worker) find Spark through
# SPARK_HOME, or else by importing pyspark with the first `python` on PATH -- Airflow's,
# which does not have it. PySpark ships the whole Spark distribution.
ENV SPARK_HOME=/opt/etl-venv/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=/opt/etl-venv/bin/python
