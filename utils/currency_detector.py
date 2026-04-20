import pandas as pd
import re


# NORMALIZACION DE MONEDA
def normalize_currency(value):
    """
    Normaliza nombres y simbolos de monedas a un formato estandar.
    Retorna: 'RD$', 'USD', 'EUR' o None
    """
    if pd.isna(value) or value is None:
        return None

    value = str(value).upper().strip()

    if not value or value in ('N/A', 'NA', 'NONE', 'NULL', '-', '—'):
        return None

    if 'RD' in value or 'PESO' in value or 'DOP' in value:
        return 'RD$'
    if 'USD' in value or 'US$' in value or 'DOLAR' in value or 'DÓLAR' in value or value == '$':
        return 'USD'
    if 'EUR' in value or 'EURO' in value or '€' in value:
        return 'EUR'

    return None


# LIMPIEZA DE MONTOS
def clean_amount(value):
    """
    Limpia un monto numerico que puede contener:
    - Simbolos de moneda
    - Formato europeo o americano
    - Texto adicional
    """
    if pd.isna(value) or value is None:
        return None

    value_str = str(value).upper().strip()

    # Eliminar simbolos de moneda (orden importante)
    for sym in ['RD$', 'US$', 'USD', 'EUR', '€', '$', 'DOP']:
        value_str = value_str.replace(sym, '')

    value_str = value_str.strip()

    if not value_str or value_str in ('N/A', 'NA', 'NULL', 'NONE', '-', '—'):
        return None

    # Detectar formato numerico
    if ',' in value_str and '.' in value_str:
        if value_str.rfind(',') > value_str.rfind('.'):
            # Formato europeo: 1.500,50
            value_str = value_str.replace('.', '').replace(',', '.')
        else:
            # Formato americano: 1,500.50
            value_str = value_str.replace(',', '')
    elif ',' in value_str:
        parts = value_str.split(',')
        if len(parts[-1]) == 2:
            value_str = value_str.replace(',', '.')
        else:
            value_str = value_str.replace(',', '')

    # Eliminar cualquier caracter no numerico restante
    value_str = re.sub(r'[^\d.-]', '', value_str)

    if not value_str:
        return None

    try:
        return float(value_str)
    except ValueError:
        return None


# DETECCIÓN DE MONEDA
def detect_currency(df: pd.DataFrame) -> str:
    """
    Detecta si el archivo contiene una sola moneda o multiples.
    Busca columnas de moneda de forma CASE-INSENSITIVE para
    que funcione aunque excel_service haya renombrado las columnas.

    Returns:
        'RD$', 'USD', 'EUR', 'Mixto' o None
        (None = no aplica, ej: módulo sanitario)
    """
    if df is None or df.empty:
        return None

    detected = set()

    # Mapa de columnas en minúsculas para búsqueda insensible a mayúsculas
    cols_lower = {c.lower(): c for c in df.columns}

    # 1. Buscar columna explícita de moneda (case-insensitive)
    currency_keys = ['moneda', 'currency', 'divisa', 'tipo_moneda', 'coin']
    for key in currency_keys:
        if key in cols_lower:
            real_col = cols_lower[key]
            for val in df[real_col].dropna().unique():
                norm = normalize_currency(val)
                if norm:
                    detected.add(norm)
            break  # Solo analizar la primera columna de moneda encontrada

    # 2. Si no se detectó por columna explícita, buscar símbolos en columnas de monto
    if not detected:
        amount_keys = ['monto', 'amount', 'precio', 'price', 'total',
                       'ingreso', 'egreso', 'importe', 'costo', 'cost']
        for key in amount_keys:
            if key in cols_lower:
                real_col = cols_lower[key]
                sample = df[real_col].dropna().astype(str).head(30)
                for val in sample:
                    norm = normalize_currency(val)
                    if norm:
                        detected.add(norm)
                if detected:
                    break

    # 3. Resultado final — None si no hay ninguna moneda detectada
    if len(detected) == 0:
        return None          # ← None en vez de 'Desconocida'
    elif len(detected) == 1:
        return list(detected)[0]
    else:
        return 'Mixto'


# TASAS DE CAMBIO
def get_exchange_rates():
    """
    Retorna tasas de cambio aproximadas de Republica Dominicana.
    En producción, debe consumirse una API oficial.
    """
    return {
        'USD_to_RD': 58.50,
        'EUR_to_RD': 63.20,
        'RD_to_USD': 0.017,
        'RD_to_EUR': 0.016
    }


# CONVERSION A RD$
def convert_to_rdp(amount, currency):
    """
    Convierte cualquier monto a Pesos Dominicanos (RD$)
    """
    if amount is None:
        return None

    currency_normalized = normalize_currency(currency)

    if not currency_normalized:
        return amount

    rates = get_exchange_rates()

    if currency_normalized == 'RD$':
        return amount
    elif currency_normalized == 'USD':
        return amount * rates['USD_to_RD']
    elif currency_normalized == 'EUR':
        return amount * rates['EUR_to_RD']
    else:
        return amount