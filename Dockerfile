FROM apache/airflow:2.7.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    unzip \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    xz-utils \
    libxml2-dev \
    libffi-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://www.python.org/ftp/python/3.10.12/Python-3.10.12.tgz && \
    tar xzf Python-3.10.12.tgz && \
    cd Python-3.10.12 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    cd .. && \
    rm -rf Python-3.10.12 Python-3.10.12.tgz

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    POETRY_VERSION=1.4.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"
USER airflow
WORKDIR /opt/airflow

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"
COPY pyproject.toml poetry.lock ./

RUN poetry env use python3.10
RUN poetry install --no-dev --no-root

COPY --chown=airflow:airflow /helpers ./helpers
COPY --chown=airflow:airflow /config ./config
COPY --chown=airflow:airflow /dags ./dags
COPY --chown=airflow:airflow gck.json ./gck.json

ENV PYTHONPATH="${WORKDIR}/helpers:${WORKDIR}/config:${PYTHONPATH}"
