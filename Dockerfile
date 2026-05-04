# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — Pipeline DataOps Telco Customer Churn
# Asignatura : ITY1101 - Gestión de Datos para IA
# ─────────────────────────────────────────────────────────────────────────────

# Imagen base oficial Python 3.11 slim (liviana, sin dependencias innecesarias)
FROM python:3.11-slim

# Metadatos del contenedor
LABEL maintainer="Equipo DataOps ITY1101"
LABEL description="Pipeline DataOps para predicción de churn en Telco"
LABEL version="1.0"

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar solo los archivos necesarios (el CSV va en /app/data/)
COPY pipeline.py .

# Crear estructura de directorios
RUN mkdir -p data output logs

# No hay dependencias externas: el pipeline usa solo la librería estándar de Python
# Si se agregan dependencias futuras (pandas, great_expectations, etc.):
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Variable de entorno para deshabilitar buffer de salida (mejor logging en Docker)
ENV PYTHONUNBUFFERED=1

# Puerto expuesto (para futuras extensiones con API REST)
# EXPOSE 8000

# Comando por defecto al ejecutar el contenedor
# El CSV debe montarse como volumen externo:
#   docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output telco-pipeline
CMD ["python", "pipeline.py", "--input", "data/telco_raw.csv", "--output", "output/"]
