# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — Pipeline DataOps Telco Customer Churn
# Basado en el devcontainer de la actividad de clases ITY1101
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="Equipo DataOps ITY1101"
LABEL description="Pipeline DataOps — Telco Customer Churn"
LABEL version="1.0"

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY pipeline.py .
COPY ingestion/ ingestion/
COPY procesamiento/ procesamiento/

# Crear estructura de directorios
RUN mkdir -p data output logs

# Sin buffer para que los logs se vean en tiempo real
ENV PYTHONUNBUFFERED=1

# Ejecutar pipeline montando volúmenes externos:
#   docker build -t telco-pipeline .
#   docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output telco-pipeline
CMD ["python", "pipeline.py", "--input", "data/telco_raw.csv", "--output", "output/"]
