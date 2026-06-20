# User-code image for the Dagster Helm deployment. The webserver + daemon run from the official
# Dagster images; this image hosts the code location (configs + framework) behind a gRPC server.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /opt/dagster/app

COPY pyproject.toml ./
COPY orchestration_dagster ./orchestration_dagster
RUN pip install -e .

EXPOSE 4000
# The Helm chart overrides this, but this is the canonical command:
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "orchestration_dagster.definitions"]
