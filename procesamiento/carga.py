"""
procesamiento/carga.py
Etapa 4 — Carga
Exporta el dataset limpio, registros rechazados y reporte de KPIs.
"""

import os
import time
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger("telco_pipeline")


def cargar_datos(
    df_valido    : pd.DataFrame,
    df_rechazado : pd.DataFrame,
    rep_limpieza : dict,
    rep_validacion: dict,
    pipeline_start: float,
    output_dir   : str = "output",
    timestamp    : str = "",
) -> dict:
    """
    Exporta los datos procesados y genera el reporte de KPIs del pipeline.

    Salidas:
      - telco_clean.csv               → Dataset listo para modelado
      - telco_rejected.csv            → Registros rechazados para auditoría
      - pipeline_report_<ts>.txt      → Reporte completo con KPIs

    Args:
        df_valido      : DataFrame con registros válidos.
        df_rechazado   : DataFrame con registros rechazados.
        rep_limpieza   : Reporte de la etapa de limpieza.
        rep_validacion : Reporte de la etapa de validación.
        pipeline_start : Timestamp de inicio del pipeline.
        output_dir     : Directorio de salida.
        timestamp      : Timestamp para nombres de archivo.

    Returns:
        Diccionario con KPIs calculados.
    """
    os.makedirs(output_dir, exist_ok=True)
    if not timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── 4.1 Exportar dataset limpio ───────────────────────────────────────
    clean_path = os.path.join(output_dir, "telco_clean.csv")
    df_valido.to_csv(clean_path, index=False, encoding="utf-8")
    logger.info(f"Dataset limpio exportado  → {clean_path}")
    logger.info(f"  Registros escritos: {len(df_valido):,}")

    # ── 4.2 Exportar rechazados ───────────────────────────────────────────
    rejected_path = os.path.join(output_dir, "telco_rejected.csv")
    if not df_rechazado.empty:
        df_rechazado.to_csv(rejected_path, index=False, encoding="utf-8")
        logger.info(f"Registros rechazados      → {rejected_path}")

    # ── 4.3 Calcular KPIs ─────────────────────────────────────────────────
    total_inicio    = rep_limpieza["total_entrada"]
    completitud_pct = len(df_valido) / total_inicio * 100 if total_inicio > 0 else 0
    tasa_rechazo    = rep_validacion["rechazados"] / total_inicio * 100
    latencia_total  = time.time() - pipeline_start

    kpis = {
        "completitud_pct" : completitud_pct,
        "tasa_rechazo_pct": tasa_rechazo,
        "latencia_seg"    : latencia_total,
        "estado_completitud": "✔ OK" if completitud_pct >= 99 else "⚠ ALERTA",
        "estado_rechazo"    : "✔ OK" if tasa_rechazo   <= 1  else "⚠ ALERTA",
        "estado_latencia"   : "✔ OK" if latencia_total  < 60  else "⚠ ALERTA",
    }

    # ── 4.4 Exportar reporte de ejecución ─────────────────────────────────
    report_path = os.path.join(output_dir, f"pipeline_report_{timestamp}.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("  REPORTE DE EJECUCIÓN — PIPELINE TELCO CHURN\n")
        f.write(f"  Fecha/Hora : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")

        f.write("[ ETAPA 1 — INGESTA ]\n")
        f.write(f"  Registros ingestados      : {total_inicio:,}\n\n")

        f.write("[ ETAPA 2 — LIMPIEZA ]\n")
        f.write(f"  TotalCharges imputados    : {rep_limpieza['total_charges_imputados']}\n")
        f.write(f"  Duplicados eliminados     : {rep_limpieza['duplicados_eliminados']}\n")
        f.write(f"  Errores de conversión     : {len(rep_limpieza['errores_conversion'])}\n\n")

        f.write("[ ETAPA 3 — VALIDACIÓN ]\n")
        f.write(f"  Registros válidos         : {rep_validacion['validos']:,}\n")
        f.write(f"  Registros rechazados      : {rep_validacion['rechazados']:,}\n")
        f.write(f"  Tasa de rechazo           : {tasa_rechazo:.2f}%\n\n")

        f.write("[ ETAPA 4 — CARGA ]\n")
        f.write(f"  Archivo limpio            : {clean_path}\n")
        f.write(f"  Archivo rechazados        : {rejected_path}\n\n")

        f.write("[ KPIs DEL PIPELINE ]\n")
        f.write(f"  Completitud               : {completitud_pct:.2f}%  (umbral >= 99%)  {kpis['estado_completitud']}\n")
        f.write(f"  Tasa de rechazo           : {tasa_rechazo:.2f}%   (umbral <= 1%)   {kpis['estado_rechazo']}\n")
        f.write(f"  Latencia total            : {latencia_total:.3f}s   (umbral < 60s)   {kpis['estado_latencia']}\n")
        f.write("\n" + "=" * 60 + "\n")
        f.write("  FIN DEL REPORTE\n")
        f.write("=" * 60 + "\n")

    logger.info(f"Reporte de ejecución      → {report_path}")

    # ── 4.5 KPIs en log ───────────────────────────────────────────────────
    logger.info("─" * 60)
    logger.info("  KPIs DEL PIPELINE")
    logger.info("─" * 60)
    logger.info(f"Completitud    : {completitud_pct:.2f}%  (umbral >= 99%)  {kpis['estado_completitud']}")
    logger.info(f"Tasa rechazo   : {tasa_rechazo:.2f}%   (umbral <= 1%)   {kpis['estado_rechazo']}")
    logger.info(f"Latencia total : {latencia_total:.3f}s   (umbral < 60s)   {kpis['estado_latencia']}")

    return kpis
