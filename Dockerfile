FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN python -m pip install --upgrade pip setuptools wheel

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install ".[spark,sasdata,modeling]"

RUN useradd --create-home --shell /usr/sbin/nologin sasrunner
USER sasrunner

WORKDIR /workspace

ENTRYPOINT ["sas-migrator"]
CMD ["--help"]
