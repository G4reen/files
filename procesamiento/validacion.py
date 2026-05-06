"""
procesamiento/validacion.py
Etapa 3 — Validación Estructural y Semántica
Valida tipos, rangos y consistencia de negocio del dataset.
"""

import logging
import pandas as pd

logger = logging.getLogger("telco_pipeline")

# Dominios válidos por columna categórica
VALID_VALUES = {
    "gender"          : {"Male", "Female"},
    "SeniorCitizen"   : {0, 1},
    "Partner"         : {"Yes", "No"},
    "Dependents"      : {"Yes", "No"},
    "PhoneService"    : {"Yes", "No"},
    "MultipleLines"   : {"Yes", "No", "No phone service"},
    "InternetService" : {"DSL", "Fiber optic", "No"},
    "OnlineSecurity"  : {"Yes", "No", "No internet service"},
    "OnlineBackup"    : {"Yes", "No", "No internet service"},
    "DeviceProtection": {"Yes", "No", "No internet service"},
    "TechSupport"     : {"Yes", "No", "No internet service"},
    "StreamingTV"     : {"Yes", "No", "No internet service"},
    "StreamingMovies" : {"Yes", "No", "No internet service"},
    "Contract"        : {"Month-to-month", "One year", "Two year"},
    "PaperlessBilling": {"Yes", "No"},
    "PaymentMethod"   : {
        "Electronic check", "Mailed check",
        "Bank transfer (automatic)", "Credit card (automatic)"
    },
    "Churn"           : {"Yes", "No"},
}

REQUIRED_COLUMNS = [
    "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
    "tenure", "PhoneService", "InternetService", "Contract",
    "MonthlyCharges", "Churn"
]

INTERNET_SERVICES = {
    "OnlineSecurity", "OnlineBackup", "DeviceProtection",
    "TechSupport", "StreamingTV", "StreamingMovies"
}


def validar_datos(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Valida el DataFrame limpio aplicando reglas estructurales y semánticas.

    Validaciones estructurales:
      - Campos requeridos no nulos
      - Rangos numéricos válidos (tenure >= 0, MonthlyCharges > 0)

    Validaciones semánticas:
      - Valores dentro del dominio definido por columna
      - Consistencia InternetService=No → servicios = 'No internet service'
      - Consistencia PhoneService=No → MultipleLines = 'No phone service'

    Args:
        df: DataFrame limpio de la etapa de limpieza.

    Returns:
        Tupla (df_valido, df_rechazado, reporte_validacion).
    """
    reporte = {
        "total_entrada": len(df),
        "validos"      : 0,
        "rechazados"   : 0,
        "errores_tipo" : {
            "campo_requerido_nulo": 0,
            "rango_invalido"      : 0,
            "valor_categorico"    : 0,
            "inconsistencia"      : 0,
        }
    }

    errores_list = []

    for idx, row in df.iterrows():
        errores = []

        # ── 3.1 Campos requeridos no nulos ────────────────────────────────
        for col in REQUIRED_COLUMNS:
            val = row.get(col)
            if pd.isna(val) or str(val).strip() == "":
                errores.append(f"REQUERIDO_NULO: '{col}' vacío")
                reporte["errores_tipo"]["campo_requerido_nulo"] += 1

        # ── 3.2 Rangos numéricos ──────────────────────────────────────────
        tenure = row.get("tenure")
        if pd.notna(tenure) and tenure < 0:
            errores.append(f"RANGO_INVALIDO: tenure={tenure} (debe ser >= 0)")
            reporte["errores_tipo"]["rango_invalido"] += 1

        monthly = row.get("MonthlyCharges")
        if pd.notna(monthly) and monthly <= 0:
            errores.append(f"RANGO_INVALIDO: MonthlyCharges={monthly} (debe ser > 0)")
            reporte["errores_tipo"]["rango_invalido"] += 1

        # ── 3.3 Valores categóricos ───────────────────────────────────────
        for col, allowed in VALID_VALUES.items():
            val = row.get(col)
            # SeniorCitizen es numérico
            if col == "SeniorCitizen":
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    pass
            if val not in allowed:
                errores.append(f"VALOR_INVALIDO: '{col}'='{val}'")
                reporte["errores_tipo"]["valor_categorico"] += 1

        # ── 3.4 Consistencia InternetService ──────────────────────────────
        if row.get("InternetService") == "No":
            for svc in INTERNET_SERVICES:
                if row.get(svc) != "No internet service":
                    errores.append(
                        f"INCONSISTENCIA: InternetService=No pero "
                        f"'{svc}'='{row.get(svc)}'"
                    )
                    reporte["errores_tipo"]["inconsistencia"] += 1

        # ── 3.5 Consistencia PhoneService ─────────────────────────────────
        if row.get("PhoneService") == "No":
            if row.get("MultipleLines") != "No phone service":
                errores.append(
                    f"INCONSISTENCIA: PhoneService=No pero "
                    f"MultipleLines='{row.get('MultipleLines')}'"
                )
                reporte["errores_tipo"]["inconsistencia"] += 1

        errores_list.append(errores)

    # Separar válidos y rechazados
    mask_valido = [len(e) == 0 for e in errores_list]
    df_valido    = df[mask_valido].copy().reset_index(drop=True)
    df_rechazado = df[~pd.Series(mask_valido)].copy().reset_index(drop=True)
    df_rechazado["errores"] = [
        " | ".join(e) for e in errores_list if len(e) > 0
    ]

    reporte["validos"]    = len(df_valido)
    reporte["rechazados"] = len(df_rechazado)
    tasa = reporte["rechazados"] / reporte["total_entrada"] * 100

    logger.info(f"Registros validados  : {reporte['validos']:,}")
    logger.info(f"Registros rechazados : {reporte['rechazados']:,} ({tasa:.2f}%)")
    logger.info("Errores por tipo:")
    for tipo, cnt in reporte["errores_tipo"].items():
        if cnt > 0:
            logger.info(f"  {tipo:<30}: {cnt}")

    # KPI: alerta si tasa de rechazo > 1%
    if tasa > 1.0:
        logger.warning(f"⚠ ALERTA KPI: Tasa de rechazo {tasa:.2f}% supera umbral 1%")
    else:
        logger.info(f"✔ KPI tasa de rechazo OK: {tasa:.2f}% (umbral: 1%)")

    return df_valido, df_rechazado, reporte
