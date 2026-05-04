"""
=============================================================================
PIPELINE DATAOPS - TELCO CUSTOMER CHURN
Asignatura : ITY1101 - Gestión de Datos para IA
Evaluación : Parcial N°2
Metodología: PMBOK Predictiva (Waterfall)
=============================================================================

Etapas del pipeline:
    1. Ingesta       → Carga del CSV fuente
    2. Limpieza      → Corrección de nulos, tipos y estandarización
    3. Validación    → Reglas estructurales y semánticas
    4. Carga         → Exportación del dataset limpio + reporte

Uso:
    python pipeline.py
    python pipeline.py --input data/telco_raw.csv --output output/
"""

import os
import csv
import time
import logging
import argparse
import hashlib
from datetime import datetime
from copy import deepcopy

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE LOGGING
# ─────────────────────────────────────────────────────────────────────────────

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file  = os.path.join(LOG_DIR, f"pipeline_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("telco_pipeline")


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTES Y REGLAS DE NEGOCIO
# ─────────────────────────────────────────────────────────────────────────────

# Valores válidos por columna categórica
VALID_VALUES = {
    "gender":            {"Male", "Female"},
    "SeniorCitizen":     {"0", "1"},
    "Partner":           {"Yes", "No"},
    "Dependents":        {"Yes", "No"},
    "PhoneService":      {"Yes", "No"},
    "MultipleLines":     {"Yes", "No", "No phone service"},
    "InternetService":   {"DSL", "Fiber optic", "No"},
    "OnlineSecurity":    {"Yes", "No", "No internet service"},
    "OnlineBackup":      {"Yes", "No", "No internet service"},
    "DeviceProtection":  {"Yes", "No", "No internet service"},
    "TechSupport":       {"Yes", "No", "No internet service"},
    "StreamingTV":       {"Yes", "No", "No internet service"},
    "StreamingMovies":   {"Yes", "No", "No internet service"},
    "Contract":          {"Month-to-month", "One year", "Two year"},
    "PaperlessBilling":  {"Yes", "No"},
    "PaymentMethod":     {
        "Electronic check", "Mailed check",
        "Bank transfer (automatic)", "Credit card (automatic)"
    },
    "Churn":             {"Yes", "No"},
}

# Columnas numéricas esperadas
NUMERIC_COLUMNS = ["tenure", "MonthlyCharges", "TotalCharges"]

# Columnas que no pueden ser nulas
REQUIRED_COLUMNS = [
    "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
    "tenure", "PhoneService", "InternetService", "Contract",
    "MonthlyCharges", "Churn"
]

EXPECTED_COLUMNS = list(VALID_VALUES.keys()) + NUMERIC_COLUMNS + ["customerID"]


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def mask_id(customer_id: str) -> str:
    """Enmascara el customerID para logs (seguridad - Ley 19.628)."""
    h = hashlib.sha256(customer_id.encode()).hexdigest()[:6]
    return f"***{h}"


def separator(title: str = ""):
    """Imprime separador visual en log."""
    line = "─" * 60
    if title:
        logger.info(f"{line}")
        logger.info(f"  {title.upper()}")
        logger.info(f"{line}")
    else:
        logger.info(line)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — INGESTA
# ─────────────────────────────────────────────────────────────────────────────

def etapa_ingesta(input_path: str) -> list[dict]:
    """
    Carga el CSV fuente y retorna lista de registros.

    Args:
        input_path: Ruta al archivo CSV de origen.

    Returns:
        Lista de diccionarios con los registros crudos.

    Raises:
        FileNotFoundError: Si el archivo no existe.
        ValueError: Si el archivo está vacío o mal formado.
    """
    separator("ETAPA 1 — INGESTA")
    start = time.time()

    if not os.path.exists(input_path):
        logger.error(f"Archivo no encontrado: {input_path}")
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    file_size_kb = os.path.getsize(input_path) / 1024
    logger.info(f"Fuente    : {input_path}")
    logger.info(f"Tamaño    : {file_size_kb:.1f} KB")

    rows = []
    with open(input_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        if not headers:
            raise ValueError("El archivo CSV no tiene encabezados.")

        logger.info(f"Columnas detectadas ({len(headers)}): {', '.join(headers)}")

        for i, row in enumerate(reader, start=2):  # línea 1 = header
            rows.append(dict(row))

    if not rows:
        raise ValueError("El archivo CSV está vacío (sin registros).")

    elapsed = time.time() - start
    logger.info(f"Registros ingestados : {len(rows):,}")
    logger.info(f"Tiempo de ingesta    : {elapsed:.3f}s")
    logger.info("✔ Etapa 1 completada exitosamente")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — LIMPIEZA Y TRANSFORMACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def etapa_limpieza(rows: list[dict]) -> tuple[list[dict], dict]:
    """
    Limpia y transforma el dataset:
      - Elimina espacios en blanco
      - Imputa TotalCharges vacío → 0.0 (clientes con tenure=0)
      - Convierte columnas numéricas a float/int
      - Estandariza strings (strip)

    Args:
        rows: Lista de registros crudos.

    Returns:
        Tupla (registros_limpios, reporte_limpieza).
    """
    separator("ETAPA 2 — LIMPIEZA Y TRANSFORMACIÓN")
    start = time.time()

    cleaned     = []
    reporte     = {
        "total_entrada"         : len(rows),
        "total_charges_imputados": 0,
        "registros_duplicados"  : 0,
        "conversiones_numericas": 0,
        "errores_conversion"    : [],
    }

    seen_ids = set()

    for i, raw in enumerate(rows, start=1):
        row = deepcopy(raw)

        # ── 2.1 Strip de espacios en todos los campos ──────────────────────
        for col in row:
            if isinstance(row[col], str):
                row[col] = row[col].strip()

        # ── 2.2 Detección de duplicados por customerID ─────────────────────
        cid = row.get("customerID", "")
        if cid in seen_ids:
            reporte["registros_duplicados"] += 1
            logger.warning(f"  Duplicado detectado → ID enmascarado: {mask_id(cid)}")
            continue
        seen_ids.add(cid)

        # ── 2.3 Imputación de TotalCharges vacío ───────────────────────────
        if row.get("TotalCharges", "") == "":
            tenure = row.get("tenure", "0")
            logger.info(
                f"  TotalCharges vacío → imputado 0.0 "
                f"[ID: {mask_id(cid)}, tenure={tenure}]"
            )
            row["TotalCharges"] = "0.0"
            reporte["total_charges_imputados"] += 1

        # ── 2.4 Conversión de tipos numéricos ──────────────────────────────
        for col in NUMERIC_COLUMNS:
            val = row.get(col, "")
            try:
                if col == "tenure":
                    row[col] = int(float(val))
                else:
                    row[col] = float(val)
                reporte["conversiones_numericas"] += 1
            except (ValueError, TypeError):
                logger.warning(
                    f"  No se pudo convertir '{col}'='{val}' "
                    f"[ID: {mask_id(cid)}] → se deja como string"
                )
                reporte["errores_conversion"].append(
                    {"fila": i, "columna": col, "valor": val}
                )

        cleaned.append(row)

    elapsed = time.time() - start
    logger.info(f"Registros entrada     : {reporte['total_entrada']:,}")
    logger.info(f"Registros limpios     : {len(cleaned):,}")
    logger.info(f"TotalCharges imputados: {reporte['total_charges_imputados']}")
    logger.info(f"Duplicados eliminados : {reporte['registros_duplicados']}")
    logger.info(f"Errores de conversión : {len(reporte['errores_conversion'])}")
    logger.info(f"Tiempo de limpieza    : {elapsed:.3f}s")
    logger.info("✔ Etapa 2 completada exitosamente")
    return cleaned, reporte


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — VALIDACIÓN ESTRUCTURAL Y SEMÁNTICA
# ─────────────────────────────────────────────────────────────────────────────

def etapa_validacion(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """
    Valida cada registro contra reglas estructurales y semánticas.

    Validaciones estructurales:
      - Campos requeridos no nulos
      - Tipos numéricos correctos
      - Rangos válidos (tenure >= 0, MonthlyCharges > 0)

    Validaciones semánticas:
      - Valores categóricos dentro del dominio definido
      - Consistencia: si InternetService=No → servicios web deben ser 'No internet service'
      - Consistencia: si PhoneService=No → MultipleLines debe ser 'No phone service'

    Args:
        rows: Lista de registros limpios.

    Returns:
        Tupla (registros_validos, registros_rechazados, reporte_validacion).
    """
    separator("ETAPA 3 — VALIDACIÓN ESTRUCTURAL Y SEMÁNTICA")
    start = time.time()

    valid    = []
    rejected = []
    reporte  = {
        "total_entrada"    : len(rows),
        "validos"          : 0,
        "rechazados"       : 0,
        "errores_por_tipo" : {
            "campo_requerido_nulo" : 0,
            "tipo_incorrecto"      : 0,
            "rango_invalido"       : 0,
            "valor_categorico"     : 0,
            "inconsistencia"       : 0,
        }
    }

    internet_services = {
        "OnlineSecurity", "OnlineBackup", "DeviceProtection",
        "TechSupport", "StreamingTV", "StreamingMovies"
    }

    for row in rows:
        errores = []
        cid = row.get("customerID", "?")

        # ── 3.1 Campos requeridos no nulos ─────────────────────────────────
        for col in REQUIRED_COLUMNS:
            val = row.get(col, "")
            if val == "" or val is None:
                errores.append(f"REQUERIDO_NULO: '{col}' está vacío")
                reporte["errores_por_tipo"]["campo_requerido_nulo"] += 1

        # ── 3.2 Validación de rangos numéricos ─────────────────────────────
        tenure = row.get("tenure", 0)
        if isinstance(tenure, (int, float)) and tenure < 0:
            errores.append(f"RANGO_INVALIDO: tenure={tenure} (debe ser >= 0)")
            reporte["errores_por_tipo"]["rango_invalido"] += 1

        monthly = row.get("MonthlyCharges", 0)
        if isinstance(monthly, (int, float)) and monthly <= 0:
            errores.append(f"RANGO_INVALIDO: MonthlyCharges={monthly} (debe ser > 0)")
            reporte["errores_por_tipo"]["rango_invalido"] += 1

        total_c = row.get("TotalCharges", 0)
        if isinstance(total_c, (int, float)) and total_c < 0:
            errores.append(f"RANGO_INVALIDO: TotalCharges={total_c} (debe ser >= 0)")
            reporte["errores_por_tipo"]["rango_invalido"] += 1

        # ── 3.3 Validación semántica: valores categóricos ──────────────────
        for col, allowed in VALID_VALUES.items():
            val = str(row.get(col, ""))
            if val not in allowed:
                errores.append(
                    f"VALOR_INVALIDO: '{col}'='{val}' "
                    f"no está en {sorted(allowed)}"
                )
                reporte["errores_por_tipo"]["valor_categorico"] += 1

        # ── 3.4 Consistencia: InternetService=No ───────────────────────────
        internet = row.get("InternetService", "")
        if internet == "No":
            for svc in internet_services:
                val = row.get(svc, "")
                if val != "No internet service":
                    errores.append(
                        f"INCONSISTENCIA: InternetService=No pero "
                        f"'{svc}'='{val}' (esperado: 'No internet service')"
                    )
                    reporte["errores_por_tipo"]["inconsistencia"] += 1

        # ── 3.5 Consistencia: PhoneService=No ──────────────────────────────
        phone = row.get("PhoneService", "")
        if phone == "No":
            ml = row.get("MultipleLines", "")
            if ml != "No phone service":
                errores.append(
                    f"INCONSISTENCIA: PhoneService=No pero "
                    f"MultipleLines='{ml}' (esperado: 'No phone service')"
                )
                reporte["errores_por_tipo"]["inconsistencia"] += 1

        # ── Clasificar registro ─────────────────────────────────────────────
        if errores:
            rejected.append({"customerID": mask_id(cid), "errores": errores})
            reporte["rechazados"] += 1
        else:
            valid.append(row)
            reporte["validos"] += 1

    elapsed = time.time() - start

    tasa_rechazo = (reporte["rechazados"] / reporte["total_entrada"] * 100
                    if reporte["total_entrada"] > 0 else 0)

    logger.info(f"Registros validados  : {reporte['validos']:,}")
    logger.info(f"Registros rechazados : {reporte['rechazados']:,} ({tasa_rechazo:.2f}%)")
    logger.info("Errores por tipo:")
    for tipo, cnt in reporte["errores_por_tipo"].items():
        if cnt > 0:
            logger.info(f"  {tipo:<30}: {cnt}")

    if rejected:
        logger.warning(f"  Primeros 3 rechazos (ID enmascarado):")
        for r in rejected[:3]:
            logger.warning(f"    ID={r['customerID']} → {r['errores'][0]}")

    # ── KPI: Alerta si tasa de rechazo supera el 1% ────────────────────────
    if tasa_rechazo > 1.0:
        logger.warning(
            f"⚠ ALERTA KPI: Tasa de rechazo {tasa_rechazo:.2f}% supera umbral del 1%"
        )
    else:
        logger.info(f"✔ KPI tasa de rechazo OK: {tasa_rechazo:.2f}% (umbral: 1%)")

    logger.info(f"Tiempo de validación : {elapsed:.3f}s")
    logger.info("✔ Etapa 3 completada exitosamente")
    return valid, rejected, reporte


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4 — CARGA
# ─────────────────────────────────────────────────────────────────────────────

def etapa_carga(
    rows: list[dict],
    rejected: list[dict],
    output_dir: str,
    reporte_limpieza: dict,
    reporte_validacion: dict,
    pipeline_start: float
) -> None:
    """
    Exporta el dataset limpio y validado, más un reporte de ejecución.

    Salidas:
      - telco_clean.csv        → Dataset procesado listo para modelado
      - telco_rejected.csv     → Registros rechazados para revisión
      - pipeline_report_<ts>.txt → Reporte de ejecución con KPIs

    Args:
        rows              : Registros válidos.
        rejected          : Registros rechazados.
        output_dir        : Directorio de salida.
        reporte_limpieza  : Métricas de la etapa 2.
        reporte_validacion: Métricas de la etapa 3.
        pipeline_start    : Timestamp de inicio del pipeline completo.
    """
    separator("ETAPA 4 — CARGA")
    start = time.time()
    os.makedirs(output_dir, exist_ok=True)

    # ── 4.1 Exportar dataset limpio ────────────────────────────────────────
    clean_path = os.path.join(output_dir, "telco_clean.csv")
    if rows:
        fieldnames = list(rows[0].keys())
        with open(clean_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Dataset limpio exportado  → {clean_path}")
        logger.info(f"  Registros escritos: {len(rows):,}")
    else:
        logger.warning("No hay registros válidos para exportar.")

    # ── 4.2 Exportar registros rechazados ──────────────────────────────────
    rejected_path = os.path.join(output_dir, "telco_rejected.csv")
    if rejected:
        with open(rejected_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["customerID", "errores"])
            writer.writeheader()
            for r in rejected:
                writer.writerow({
                    "customerID": r["customerID"],
                    "errores"   : " | ".join(r["errores"])
                })
        logger.info(f"Registros rechazados      → {rejected_path}")
        logger.info(f"  Registros rechazados: {len(rejected):,}")

    # ── 4.3 Calcular KPIs del pipeline ────────────────────────────────────
    total_inicio    = reporte_limpieza["total_entrada"]
    total_final     = len(rows)
    completitud_pct = (total_final / total_inicio * 100) if total_inicio > 0 else 0
    tasa_rechazo    = reporte_validacion["rechazados"] / total_inicio * 100
    latencia_total  = time.time() - pipeline_start

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
        f.write(f"  TotalCharges imputados    : {reporte_limpieza['total_charges_imputados']}\n")
        f.write(f"  Duplicados eliminados     : {reporte_limpieza['registros_duplicados']}\n")
        f.write(f"  Errores de conversión     : {len(reporte_limpieza['errores_conversion'])}\n\n")

        f.write("[ ETAPA 3 — VALIDACIÓN ]\n")
        f.write(f"  Registros válidos         : {reporte_validacion['validos']:,}\n")
        f.write(f"  Registros rechazados      : {reporte_validacion['rechazados']:,}\n")
        f.write(f"  Tasa de rechazo           : {tasa_rechazo:.2f}%\n\n")

        f.write("[ ETAPA 4 — CARGA ]\n")
        f.write(f"  Archivo limpio            : {clean_path}\n")
        f.write(f"  Archivo rechazados        : {rejected_path}\n\n")

        f.write("[ KPIs DEL PIPELINE ]\n")
        f.write(f"  Completitud               : {completitud_pct:.2f}% (umbral >= 99%)\n")
        f.write(f"  Tasa de rechazo           : {tasa_rechazo:.2f}% (umbral <= 1%)\n")
        f.write(f"  Latencia total            : {latencia_total:.3f}s (umbral < 60s)\n")
        estado_completitud = "✔ OK" if completitud_pct >= 99 else "⚠ ALERTA"
        estado_rechazo     = "✔ OK" if tasa_rechazo   <= 1  else "⚠ ALERTA"
        estado_latencia    = "✔ OK" if latencia_total  < 60  else "⚠ ALERTA"
        f.write(f"  Estado completitud        : {estado_completitud}\n")
        f.write(f"  Estado tasa rechazo       : {estado_rechazo}\n")
        f.write(f"  Estado latencia           : {estado_latencia}\n")
        f.write("\n" + "=" * 60 + "\n")
        f.write("  FIN DEL REPORTE\n")
        f.write("=" * 60 + "\n")

    logger.info(f"Reporte de ejecución      → {report_path}")

    # ── 4.5 Resumen de KPIs en log ────────────────────────────────────────
    separator("KPIs DEL PIPELINE")
    logger.info(f"Completitud    : {completitud_pct:.2f}%  (umbral >= 99%)  {estado_completitud}")
    logger.info(f"Tasa rechazo   : {tasa_rechazo:.2f}%   (umbral <= 1%)   {estado_rechazo}")
    logger.info(f"Latencia total : {latencia_total:.3f}s   (umbral < 60s)   {estado_latencia}")

    elapsed = time.time() - start
    logger.info(f"Tiempo de carga      : {elapsed:.3f}s")
    logger.info("✔ Etapa 4 completada exitosamente")


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def main():
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

    pipeline_start = time.time()

    separator("INICIO DEL PIPELINE DATAOPS — TELCO CUSTOMER CHURN")
    logger.info(f"Log guardado en: {log_file}")
    logger.info(f"Input  : {args.input}")
    logger.info(f"Output : {args.output}")

    # Ejecutar las 4 etapas
    raw_rows                      = etapa_ingesta(args.input)
    clean_rows, rep_limpieza      = etapa_limpieza(raw_rows)
    valid_rows, rejected, rep_val = etapa_validacion(clean_rows)
    etapa_carga(
        valid_rows, rejected, args.output,
        rep_limpieza, rep_val, pipeline_start
    )

    separator("PIPELINE FINALIZADO")
    logger.info(
        f"Duración total: {time.time() - pipeline_start:.3f}s  |  "
        f"Registros finales: {len(valid_rows):,}  |  "
        f"Log: {log_file}"
    )


if __name__ == "__main__":
    main()
