FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml ./
COPY error_analyzer ./error_analyzer
RUN pip install -e .

# Cloud Run injects PORT; default 8080 for local.
ENV PORT=8080
CMD ["sh", "-c", "uvicorn error_analyzer.server:app --host 0.0.0.0 --port ${PORT}"]
