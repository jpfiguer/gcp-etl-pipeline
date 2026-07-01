# imagen para correr el pipeline en dataflow flex templates o localmente en docker
FROM python:3.12-slim

WORKDIR /app

# instalar dependencias del sistema (necesarias para algunas libs de gcp)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# instalar dependencias python
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e .

# entrypoint por default: streaming pipeline con DirectRunner
CMD ["python", "-m", "src.pipeline.streaming", "--runner", "DirectRunner", "--streaming"]
