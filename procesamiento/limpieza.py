"""
procesamiento/limpieza.py
Etapa 2 — Limpieza y Transformación
Corrige nulos, estandariza tipos y elimina duplicados.
"""

import hashlib
import logging
import pandas as pd

logger = logging.getLogger("telco_pipeline")

NUMERIC_COLUMNS = ["tenure", "MonthlyCharges", "TotalCharges"]


def _mask_id(customer_id: str) -> str:
    """Enmascara customerID para logs (seguridad — Ley 19.628)."""
    return "***" + hashlib.sha256(customer_id.encode()).hexdigest()[:6]


def limpiar_datos(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Limpia y transforma el DataFrame crudo:
      - Strip de espacios en strings
      - Eliminación de duplicados por customerID
      - Imputación de TotalCharges vacío → 0.0 (clientes con tenure=0)
      - Conversión de columnas numéricas a tipos correctos

    Args:
        df: DataFrame crudo de la etapa de ingesta.

    Returns:
        Tupla (DataFrame limpio, reporte de limpieza).
    """
    reporte = {
        "total_entrada"          : len(df),
        "duplicados_eliminados"  : 0,
        "total_charges_imputados": 0,
        "errores_conversion"     : [],
    }

    # ── 2.1 Strip de espacios en columnas string ───────────────────────────
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # ── 2.2 Eliminación de duplicados ─────────────────────────────────────
    antes = len(df)
    df = df.drop_duplicates(subset=["customerID"])
    reporte["duplicados_eliminados"] = antes - len(df)
    if reporte["duplicados_eliminados"] > 0:
        logger.warning(f"  Duplicados eliminados: {reporte['duplicados_eliminados']}")

    # ── 2.3 Imputación de TotalCharges vacío ──────────────────────────────
    mask_vacio = df["TotalCharges"].replace("", pd.NA).isna()
    for idx in df[mask_vacio].index:
        cid    = df.at[idx, "customerID"]
        tenure = df.at[idx, "tenure"]
        logger.info(
            f"  TotalCharges vacío → imputado 0.0 "
            f"[ID: {_mask_id(str(cid))}, tenure={tenure}]"
        )
    df.loc[mask_vacio, "TotalCharges"] = "0.0"
    reporte["total_charges_imputados"] = int(mask_vacio.sum())

    # ── 2.4 Conversión de tipos numéricos ─────────────────────────────────
    for col in NUMERIC_COLUMNS:
        try:
            if col == "tenure":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        except Exception as e:
            logger.warning(f"  Error convirtiendo '{col}': {e}")
            reporte["errores_conversion"].append({"columna": col, "error": str(e)})

    logger.info(f"Registros entrada     : {reporte['total_entrada']:,}")
    logger.info(f"Registros limpios     : {len(df):,}")
    logger.info(f"TotalCharges imputados: {reporte['total_charges_imputados']}")
    logger.info(f"Duplicados eliminados : {reporte['duplicados_eliminados']}")
    logger.info(f"Errores de conversión : {len(reporte['errores_conversion'])}")

    return df, reporte
