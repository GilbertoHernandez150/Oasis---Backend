import pandas as pd
import numpy as np
import re


# ============================================================
# EXCEL SERVICE — Limpieza y validación de datos
# Se ejecuta SIEMPRE antes de cualquier análisis.
# ============================================================

# Columnas numéricas conocidas que nunca deben ser cero o NaN
# para considerarse un registro válido
NUMERIC_CRITICAL = [
    'monto', 'amount', 'precio', 'price', 'total',
    'ingreso', 'egreso', 'importe', 'costo', 'cost',
    'dias_perdidos', 'days_lost', 'dias',
]


def clean_dataframe(df: pd.DataFrame, modulo: str = 'auto') -> dict:
    """
    Limpia y valida un DataFrame antes del análisis.

    Retorna:
        {
            'df':      DataFrame limpio,
            'reporte': dict con estadísticas de limpieza,
            'error':   str | None
        }
    """
    if df is None or df.empty:
        return {'df': df, 'reporte': {}, 'error': 'El archivo está vacío'}

    reporte = {
        'filas_originales':    len(df),
        'columnas_originales': len(df.columns),
        'filas_duplicadas':    0,
        'filas_nulas':         0,
        'valores_corregidos':  0,
        'filas_finales':       0,
        'advertencias':        [],
    }

    # ── 1. Limpiar nombres de columnas ──────────────────────
    df.columns = [_clean_col_name(c) for c in df.columns]

    # ── 2. Eliminar columnas completamente vacías ────────────
    cols_antes = len(df.columns)
    df = df.dropna(axis=1, how='all')
    cols_vacias = cols_antes - len(df.columns)
    if cols_vacias > 0:
        reporte['advertencias'].append(f'Se eliminaron {cols_vacias} columna(s) completamente vacías')

    # ── 3. Eliminar filas completamente vacías ───────────────
    filas_antes = len(df)
    df = df.dropna(how='all')
    filas_vacias = filas_antes - len(df)
    if filas_vacias > 0:
        reporte['advertencias'].append(f'Se eliminaron {filas_vacias} fila(s) completamente vacías')

    # ── 4. Eliminar duplicados exactos ───────────────────────
    filas_antes = len(df)
    df = df.drop_duplicates()
    reporte['filas_duplicadas'] = filas_antes - len(df)
    if reporte['filas_duplicadas'] > 0:
        reporte['advertencias'].append(
            f'Se eliminaron {reporte["filas_duplicadas"]} fila(s) duplicada(s) exactas'
        )

    # ── 5. Limpiar y convertir columnas numéricas ────────────
    for col in df.columns:
        if _is_likely_numeric(df[col]):
            original_nulls = df[col].isna().sum()
            df[col] = df[col].apply(_parse_numeric)
            new_nulls = df[col].isna().sum()
            converted = int(original_nulls - new_nulls) if new_nulls < original_nulls else 0
            if converted > 0:
                reporte['valores_corregidos'] += converted

    # ── 6. Limpiar columnas de texto (strip, normalizar N/A) ─
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(_clean_text_value)

    # ── 7. Limpiar fechas ────────────────────────────────────
    for col in df.columns:
        if _is_likely_date(col, df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # ── 8. Reemplazar NaN restantes por valores seguros ──────
    # NaN en texto → None (se serializa como null en JSON)
    # NaN en números → 0 para columnas operativas, None para el resto
    df = _fill_remaining_nans(df, modulo)

    # ── 9. Contar filas con demasiados nulos (>50% de cols) ──
    umbral_nulos = len(df.columns) * 0.5
    filas_con_muchos_nulos = (df.isnull().sum(axis=1) > umbral_nulos).sum()
    reporte['filas_nulas'] = int(filas_con_muchos_nulos)
    if filas_con_muchos_nulos > 0:
        reporte['advertencias'].append(
            f'{filas_con_muchos_nulos} fila(s) tienen más del 50% de valores vacíos'
        )

    reporte['filas_finales'] = len(df)

    return {'df': df, 'reporte': reporte, 'error': None}


# ============================================================
# SERIALIZACIÓN SEGURA PARA JSON
# Convierte NaN, Infinity y otros valores no-JSON a None
# ============================================================
def safe_json_value(obj):
    """
    Convierte recursivamente un objeto Python a algo
    serializable en JSON: NaN → None, inf → None.
    """
    if isinstance(obj, dict):
        return {k: safe_json_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_json_value(i) for i in obj]
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return round(obj, 4)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return round(float(obj), 4)
    if isinstance(obj, np.ndarray):
        return safe_json_value(obj.tolist())
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    if pd.isna(obj) if not isinstance(obj, (list, dict, str, bool)) else False:
        return None
    return obj


# ============================================================
# HELPERS PRIVADOS
# ============================================================

def _clean_col_name(name: str) -> str:
    """Normaliza el nombre de una columna."""
    name = str(name).strip()
    name = re.sub(r'\s+', '_', name)          # espacios → _
    name = re.sub(r'[^\w]', '_', name)        # caracteres especiales → _
    name = re.sub(r'_+', '_', name)           # múltiples _ → uno
    name = name.strip('_')
    return name


def _is_likely_numeric(series: pd.Series) -> bool:
    """
    Detecta si una columna debería ser numérica.
    Excluye columnas de moneda, fechas y categorías de texto.
    """
    if series.dtype in [np.float64, np.int64, float, int]:
        return True
    if series.dtype in ['datetime64[ns]', 'datetime64']:
        return False

    sample = series.dropna().astype(str).head(20)
    if len(sample) == 0:
        return False

    # Excluir columnas de moneda (RD$, USD, EUR, etc.)
    currency_words = {'RD$', 'USD', 'EUR', 'DOP', 'US$', 'PESO', 'DOLAR', 'EURO'}
    currency_count = sum(1 for v in sample if v.strip().upper() in currency_words)
    if currency_count / len(sample) > 0.4:
        return False

    # Excluir columnas de fecha (YYYY-MM-DD, DD/MM/YYYY, etc.)
    date_count = sum(
        1 for v in sample
        if re.match(r'\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}', v.strip())
    )
    if date_count / len(sample) > 0.4:
        return False

    # Patrón numérico puro: solo dígitos, comas, puntos y signo
    numeric_count = sum(
        1 for v in sample
        if re.match(r'^[\d,\.\s\-\+]+[%]?$', v.strip())
    )
    return numeric_count / len(sample) > 0.6


def _parse_numeric(value) -> float:
    """Intenta parsear un valor a float, manejando formatos RD/EUR/US."""
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        if np.isnan(value) or np.isinf(value):
            return np.nan
        return float(value)

    v = str(value).strip()

    # Eliminar símbolos de moneda y texto
    for sym in ['RD$', 'US$', 'USD', 'EUR', 'DOP', '$', '€', '£', '%']:
        v = v.replace(sym, '')
    v = v.strip()

    if not v or v.upper() in ('N/A', 'NA', 'NULL', 'NONE', '-', '—', ''):
        return np.nan

    # Formato europeo: 1.500,50
    if ',' in v and '.' in v:
        if v.rfind(',') > v.rfind('.'):
            v = v.replace('.', '').replace(',', '.')
        else:
            v = v.replace(',', '')
    elif ',' in v:
        parts = v.split(',')
        if len(parts[-1]) <= 2:
            v = v.replace(',', '.')
        else:
            v = v.replace(',', '')

    v = re.sub(r'[^\d.\-]', '', v)
    try:
        result = float(v)
        return np.nan if np.isnan(result) or np.isinf(result) else result
    except ValueError:
        return np.nan


def _is_likely_date(col_name: str, series: pd.Series) -> bool:
    """Detecta si una columna es probablemente una fecha."""
    name_hints = ['fecha', 'date', 'dia', 'day', 'mes', 'month', 'año', 'year', 'periodo']
    name_lower = col_name.lower()
    if not any(h in name_lower for h in name_hints):
        return False
    if series.dtype in ['datetime64[ns]', 'datetime64']:
        return True
    sample = series.dropna().astype(str).head(10)
    date_count = sum(
        1 for v in sample
        if re.match(r'\d{1,4}[-/\.]\d{1,2}[-/\.]\d{1,4}', v)
    )
    return date_count / max(len(sample), 1) > 0.5


def _clean_text_value(value):
    """Limpia un valor de texto: strip, N/A → None."""
    if pd.isna(value):
        return None
    v = str(value).strip()
    if v.upper() in ('N/A', 'NA', 'NULL', 'NONE', '', 'NAN'):
        return None
    return v


def _fill_remaining_nans(df: pd.DataFrame, modulo: str) -> pd.DataFrame:
    """
    Rellena NaN restantes de forma inteligente según el tipo de columna:
    - Columnas operativas numéricas (monto, días, costo) → 0
    - Columnas de texto → None (null en JSON)
    - Fechas → None
    """
    cols_lower = {c.lower(): c for c in df.columns}

    for col in df.columns:
        col_l = col.lower()
        if df[col].dtype in [np.float64, np.int64, float, int]:
            # Columnas operativas → 0
            if any(k in col_l for k in NUMERIC_CRITICAL):
                df[col] = df[col].fillna(0)
            else:
                # Otras numéricas → dejar como None (será null en JSON)
                df[col] = df[col].where(df[col].notna(), other=None)
        elif df[col].dtype == object:
            # Ya limpiadas en _clean_text_value — dejar None
            pass
        # datetime → dejar como NaT (se maneja en safe_json_value)

    return df