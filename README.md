# Pipeline DataOps — Telco Customer Churn
**ITY1101 · Gestión de Datos para IA · Evaluación Parcial N°2**

## Descripción
Pipeline DataOps para procesar el dataset Telco Customer Churn. Implementa las 4 etapas del ciclo de vida del dato: Ingesta → Limpieza → Validación → Carga.

## Estructura del proyecto
```
telco_pipeline/
├── pipeline.py          # Pipeline principal (4 etapas DataOps)
├── Dockerfile           # Containerización del pipeline
├── README.md            # Este archivo
├── data/
│   └── telco_raw.csv    # Dataset fuente (7.043 registros)
├── output/
│   ├── telco_clean.csv          # Dataset procesado
│   ├── telco_rejected.csv       # Registros rechazados
│   └── pipeline_report_*.txt   # Reporte de KPIs por ejecución
└── logs/
    └── pipeline_*.log           # Logs detallados por ejecución
```

## Ejecución local
```bash
# Clonar repositorio
git clone https://github.com/<usuario>/telco-churn-pipeline.git
cd telco-churn-pipeline

# Ejecutar pipeline
python pipeline.py

# Con rutas personalizadas
python pipeline.py --input data/telco_raw.csv --output output/
```

## Ejecución con Docker
```bash
# Construir imagen
docker build -t telco-pipeline .

# Ejecutar contenedor (monta volúmenes locales)
docker run \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/output:/app/output \
  -v $(pwd)/logs:/app/logs \
  telco-pipeline
```

## Etapas del Pipeline

| Etapa | Descripción | Herramienta |
|-------|-------------|-------------|
| 1. Ingesta | Carga del CSV fuente, detección de esquema | Python csv |
| 2. Limpieza | Imputación de nulos, conversión de tipos | Python stdlib |
| 3. Validación | Reglas estructurales y semánticas, KPI alerta | Python stdlib |
| 4. Carga | Exportación CSV limpio + reporte de ejecución | Python csv |

## KPIs de Monitoreo

| KPI | Umbral | Descripción |
|-----|--------|-------------|
| Completitud | ≥ 99% | Registros válidos / total ingestados |
| Tasa de rechazo | ≤ 1% | Registros rechazados por validación |
| Latencia total | < 60s | Tiempo total de ejecución del pipeline |

## Seguridad
- Los customerID se **enmascaran con SHA-256** en los logs (Ley 19.628)
- El dataset procesado no incluye datos de identificación en logs
- Control de acceso por roles recomendado para entornos productivos

## Resultados de la última ejecución
- Registros ingestados: **7.043**
- TotalCharges imputados: **11** (clientes con tenure=0)
- Registros válidos: **7.043** (100%)
- Latencia: **~0.27 segundos**
- Todos los KPIs en estado ✔ OK
