"""
pipeline.py — Orquestador principal
Telco Customer Churn — Pipeline DataOps
ITY1101 · Gestión de Datos para IA · Evaluación Parcial N°2

Ejecuta las 4 etapas del ciclo de vida del dato:
    1. Ingesta       → ingestion/lectura_csv.py
    2. Limpieza      → procesamiento/limpieza.py
    3. Validación    → procesamiento/validacion.py
    4. Carga         → procesamiento/carga.py

Uso:
    python pipeline.py
    python pipeline.py --input data/telco_raw.csv --output output/
"""

import os
import time
import logging
import argparse
from datetime import datetime

from ingestion.lectura_csv import leer_datos_csv
from procesamiento.limpieza import limpiar_datos
from procesamiento.validacion import validar_datos
from procesamiento.carga import cargar_datos

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE LOGGING
# ─────────────────────────────────────────────────────────────────────────────

LOG_DIR   = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE  = os.path.join(LOG_DIR, f"pipeline_{TIMESTAMP}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telco_pipeline")


def separador(titulo: str = ""):
    linea = "─" * 60
    logger.info(linea)
    if titulo:
        logger.info(f"  {titulo.upper()}")
        logger.info(linea)


# ─────────────────────────────────────────────────────────────────────────────
# ORQUESTADOR — mismo patrón que la actividad de clases
# ─────────────────────────────────────────────────────────────────────────────

def run_orchestrator(input_path: str, output_dir: str) -> dict:
    """
    Orquesta las 4 etapas del pipeline DataOps.

    Args:
        input_path : Ruta al CSV fuente.
        output_dir : Directorio de salida para archivos procesados.

    Returns:
        Diccionario con almacen_datos (DataFrames) y KPIs del pipeline.
    """
    pipeline_start = time.time()
    almacen_datos  = {}

    separador("INICIO DEL PIPELINE DATAOPS — TELCO CUSTOMER CHURN")
    logger.info(f"Log guardado en : {LOG_FILE}")
    logger.info(f"Input           : {input_path}")
    logger.info(f"Output          : {output_dir}")

    # ── ETAPA 1 — INGESTA ─────────────────────────────────────────────────
    separador("Etapa 1 — Ingesta")
    almacen_datos["telco_raw"] = leer_datos_csv(source=input_path)
    logger.info("✔ Etapa 1 completada")

    # ── Resumen datos crudos (igual que tu actividad) ─────────────────────
    logger.info("\n--- Resumen de datos sin transformar")
    df_raw = almacen_datos["telco_raw"]
    logger.info(f"FUENTE: telco_raw | Rows: {len(df_raw)} | Columns: {len(df_raw.columns)}")
    logger.info(f"\n{df_raw.head(2).to_string()}")

    # ── ETAPA 2 — LIMPIEZA ────────────────────────────────────────────────
    separador("Etapa 2 — Limpieza y Transformación")
    almacen_datos["telco_clean"], rep_limpieza = limpiar_datos(df_raw.copy())
    logger.info("✔ Etapa 2 completada")

    # ── ETAPA 3 — VALIDACIÓN ──────────────────────────────────────────────
    separador("Etapa 3 — Validación Estructural y Semántica")
    almacen_datos["telco_valid"], almacen_datos["telco_rejected"], rep_validacion = \
        validar_datos(almacen_datos["telco_clean"])
    logger.info("✔ Etapa 3 completada")

    # ── ETAPA 4 — CARGA ───────────────────────────────────────────────────
    separador("Etapa 4 — Carga")
    kpis = cargar_datos(
        df_valido      = almacen_datos["telco_valid"],
        df_rechazado   = almacen_datos["telco_rejected"],
        rep_limpieza   = rep_limpieza,
        rep_validacion = rep_validacion,
        pipeline_start = pipeline_start,
        output_dir     = output_dir,
        timestamp      = TIMESTAMP,
    )
    almacen_datos["kpis"] = kpis
    logger.info("✔ Etapa 4 completada")

    # ── Resumen final (igual que tu actividad) ────────────────────────────
    separador("Resumen de datos final")
    for nombre, df in almacen_datos.items():
        if nombre == "kpis":
            continue
        if hasattr(df, "empty") and not df.empty:
            logger.info(f"FUENTE/TRANSFORMACIÓN: {nombre} | Rows: {len(df)}")
        else:
            logger.info(f"FUENTE/TRANSFORMACIÓN: {nombre} | Sin datos")

    separador("PIPELINE FINALIZADO")
    logger.info(
        f"Duración total  : {time.time() - pipeline_start:.3f}s  |  "
        f"Registros finales: {len(almacen_datos['telco_valid']):,}  |  "
        f"Log: {LOG_FILE}"
    )

    return almacen_datos


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline DataOps — Telco Customer Churn"
    )
    parser.add_argument(
        "--input",
        default=os.path.join(os.path.dirname(__file__), "data", "telco_raw.csv"),
        help="Ruta al CSV de entrada (default: data/telco_raw.csv)"
    )
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__), "output"),
        help="Directorio de salida (default: output/)"
    )
    args = parser.parse_args()

    run_orchestrator(input_path=args.input, output_dir=args.output)
