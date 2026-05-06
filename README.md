# Pipeline DataOps — Telco Customer Churn
**ITY1101 · Gestión de Datos para IA · Evaluación Parcial N°2**

## Descripción
Pipeline DataOps para procesar el dataset Telco Customer Churn. Implementa las 4 etapas del ciclo de vida del dato: Ingesta → Limpieza → Validación → Carga.

## Estructura del proyecto
```
telco_pipeline/
├── pipeline.py                      # Orquestador principal (4 etapas DataOps)
├── Dockerfile                       # Containerización del pipeline
├── requirements.txt                 # Dependencias Python
├── README.md                        # Este archivo
├── ingestion/
│   ├── __init__.py
│   └── lectura_csv.py               # Etapa 1 — Ingesta
├── procesamiento/
│   ├── __init__.py
│   ├── limpieza.py                  # Etapa 2 — Limpieza y Transformación
│   ├── validacion.py                # Etapa 3 — Validación Estructural y Semántica
│   └── carga.py                     # Etapa 4 — Carga
├── data/
│   └── telco_raw.csv                # Dataset fuente (7.043 registros)
├── output/
│   ├── telco_clean.csv              # Dataset procesado
│   └── pipeline_report_*.txt        # Reporte de KPIs por ejecución
└── logs/
    └── pipeline_*.log               # Logs detallados por ejecución
```

## Instalación y ejecución local

```bash
pip install -r requirements.txt
python pipeline.py
```

### Con rutas personalizadas
```bash
python pipeline.py --input data/telco_raw.csv --output output/
```

## Ejecución con Docker
```bash
# Construir imagen
docker build -t telco-pipeline .

# Ejecutar contenedor
docker run \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/logs:/app/logs \
  telco-pipeline
```

## Etapas del Pipeline

| Etapa | Módulo | Descripción |
|-------|--------|-------------|
| 1. Ingesta | `ingestion/lectura_csv.py` | Carga del CSV fuente, detección de esquema |
| 2. Limpieza | `procesamiento/limpieza.py` | Imputación de nulos, conversión de tipos |
| 3. Validación | `procesamiento/validacion.py` | Reglas estructurales y semánticas |
| 4. Carga | `procesamiento/carga.py` | Exportación CSV limpio + reporte de KPIs |

## KPIs de Monitoreo

| KPI | Umbral | Descripción |
|-----|--------|-------------|
| Completitud | ≥ 99% | Registros válidos / total ingestados |
| Tasa de rechazo | ≤ 1% | Registros rechazados por validación |
| Latencia total | < 60s | Tiempo total de ejecución del pipeline |

## Seguridad
- Los `customerID` se **enmascaran con SHA-256** en los logs (Ley 19.628)
- El dataset procesado no expone identificadores en logs
- Control de acceso por roles recomendado para entornos productivos

## Resultados de la última ejecución
- Registros ingestados: **7.043**
- TotalCharges imputados: **11** (clientes con tenure=0)
- Registros válidos: **7.043** (100%)
- Latencia: **~0.65 segundos**
- Todos los KPIs en estado ✔ OK
