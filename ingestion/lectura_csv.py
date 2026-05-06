"""
ingestion/lectura_csv.py
Etapa 1 — Ingesta
Carga el CSV fuente de Telco Customer Churn y retorna un DataFrame crudo.
"""
import pandas as pd
import os
import logging


logger = logging.getLogger("telco_pipeline")


def leer_datos_csv(source: str = "data/telco_raw.csv") -> pd.DataFrame:
    """
    Lee el archivo CSV de Telco Customer Churn.

    Args:
        source: Ruta al archivo CSV de origen.

    Returns:
        DataFrame con los datos crudos.

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si el archivo está vacío.
    """
    if not os.path.exists(source):
        logger.error(f"Archivo no encontrado: {source}")
        raise FileNotFoundError(f"No se encontró el archivo: {source}")

    file_size_kb = os.path.getsize(source) / 1024
    logger.info(f"Fuente     : {source}")
    logger.info(f"Tamaño     : {file_size_kb:.1f} KB")

    df = pd.read_csv(source, encoding="utf-8")

    if df.empty:
        raise ValueError("El archivo CSV está vacío.")

    logger.info(f"Registros ingestados : {len(df):,}")
    logger.info(f"Columnas detectadas  : {len(df.columns)} → {list(df.columns)}")

    return df
