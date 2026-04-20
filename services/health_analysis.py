import pandas as pd
import numpy as np
from datetime import datetime


# ============================================================
# CONSTANTES — Normativa República Dominicana
# Ley 87-01 (Seguridad Social) y Decreto 522-06
# Jornada laboral: 44 h/semana × 50 semanas = 2,200 h/empleado/año
# ============================================================
HORAS_POR_EMPLEADO_ANIO = 2_200   # Art. 147 Código de Trabajo RD
MESES_ES = {
    1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril',
    5:'Mayo', 6:'Junio', 7:'Julio', 8:'Agosto',
    9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'
}


# ============================================================
# NORMALIZACIÓN DE COLUMNAS
# Acepta nombres con variaciones de mayúsculas/espacios/guiones
# ============================================================
ALIAS = {
    'fecha':           ['fecha', 'date', 'fecha_evento', 'fecha_reporte'],
    'departamento':    ['departamento', 'area', 'depto', 'department', 'seccion'],
    'tipo_evento':     ['tipo_evento', 'tipo', 'type', 'categoria', 'event_type'],
    'diagnostico':     ['diagnostico', 'diagnostico_cie', 'diagnosis', 'descripcion', 'enfermedad'],
    'gravedad':        ['gravedad', 'severity', 'nivel_gravedad', 'nivel'],
    'dias_perdidos':   ['dias_perdidos', 'dias', 'days_lost', 'dias_ausencia', 'jornadas_perdidas'],
    'costo_rd':        ['costo_rd', 'costo', 'cost', 'monto', 'costo_total', 'importe'],
    'empleados_dept':  ['empleados_dept', 'empleados', 'employees', 'num_empleados', 'cantidad_empleados'],
    'reportado_arl':   ['reportado_arl', 'arl', 'reportado', 'reported'],
    'resuelto':        ['resuelto', 'resolved', 'cerrado', 'closed'],
}

TIPO_ACCIDENTE  = ['accidente', 'accident', 'accidentes']
TIPO_ENFERMEDAD = ['enfermedad', 'disease', 'enfermedades', 'ocupacional']
TIPO_PREVENCION = ['prevención', 'prevencion', 'prevention', 'preventivo']


def _map_columns(df: pd.DataFrame) -> dict:
    """
    Devuelve un dict {campo_canonico: nombre_real_en_df} para las
    columnas que se encuentren. Las que no existen no aparecen.
    """
    cols_lower = {c.lower().replace(' ', '_').replace('-', '_'): c for c in df.columns}
    mapping    = {}
    for canonical, aliases in ALIAS.items():
        for alias in aliases:
            if alias in cols_lower:
                mapping[canonical] = cols_lower[alias]
                break
    return mapping


def _normalize_tipo(val: str) -> str:
    """Normaliza el tipo de evento a Accidente | Enfermedad | Prevención | Otro"""
    v = str(val).lower().strip()
    if any(t in v for t in TIPO_ACCIDENTE):
        return 'Accidente'
    if any(t in v for t in TIPO_ENFERMEDAD):
        return 'Enfermedad'
    if any(t in v for t in TIPO_PREVENCION):
        return 'Prevención'
    return 'Otro'


def _safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================
def analyze_health_data(df: pd.DataFrame) -> dict:
    """
    Analiza un DataFrame de salud ocupacional y retorna un dict
    con todos los indicadores necesarios para el dashboard.

    Columnas mínimas requeridas: Tipo_Evento, Dias_Perdidos
    Columnas opcionales: Fecha, Departamento, Diagnostico,
                         Gravedad, Costo_RD, Empleados_Dept
    """
    if df is None or df.empty:
        return {"error": "El archivo está vacío"}

    col = _map_columns(df)

    # ── Validar mínimo requerido ────────────────────────────
    if 'tipo_evento' not in col:
        return {
            "error": "Columna requerida no encontrada: Tipo_Evento. "
                     "El archivo debe contener una columna que identifique "
                     "el tipo de evento (Accidente, Enfermedad, Prevención)."
        }

    # ── Copia de trabajo ────────────────────────────────────
    data = df.copy()

    # ── Normalizar tipo de evento ───────────────────────────
    data['_tipo'] = data[col['tipo_evento']].apply(_normalize_tipo)

    # ── Días perdidos ───────────────────────────────────────
    if 'dias_perdidos' in col:
        data['_dias'] = pd.to_numeric(data[col['dias_perdidos']], errors='coerce').fillna(0)
    else:
        data['_dias'] = 0

    # ── Costo ───────────────────────────────────────────────
    if 'costo_rd' in col:
        data['_costo'] = pd.to_numeric(data[col['costo_rd']], errors='coerce').fillna(0)
    else:
        data['_costo'] = 0

    # ── Empleados por departamento ──────────────────────────
    if 'empleados_dept' in col:
        data['_empl'] = pd.to_numeric(data[col['empleados_dept']], errors='coerce').fillna(0)
        total_empleados = int(data.groupby(col['departamento'])['_empl'].first().sum()) if 'departamento' in col else int(data['_empl'].max())
    else:
        total_empleados = 100   # estimado por defecto

    # ── Fecha ───────────────────────────────────────────────
    if 'fecha' in col:
        data['_fecha'] = pd.to_datetime(data[col['fecha']], errors='coerce')
    else:
        data['_fecha'] = pd.NaT

    # ── Subtotales por tipo ─────────────────────────────────
    mask_acc  = data['_tipo'] == 'Accidente'
    mask_enf  = data['_tipo'] == 'Enfermedad'
    mask_prev = data['_tipo'] == 'Prevención'

    total_eventos      = len(data)
    total_accidentes   = int(mask_acc.sum())
    total_enfermedades = int(mask_enf.sum())
    total_prevencion   = int(mask_prev.sum())
    total_dias         = int(data['_dias'].sum())
    total_costo        = round(float(data['_costo'].sum()), 2)

    # ── Horas trabajadas totales (base para indicadores) ───
    # Ley 87-01 RD: 44 h/semana → 2,200 h/empleado/año
    horas_trabajadas = total_empleados * HORAS_POR_EMPLEADO_ANIO

    # ── INDICADORES DE SEGURIDAD (Decreto 522-06) ──────────
    # Tasa de Frecuencia  = (accidentes / horas_trabajadas) × 1,000,000
    # Tasa de Severidad   = (dias_perdidos / horas_trabajadas) × 1,000
    # Tasa de Incidencia  = (total_eventos / total_empleados) × 100
    # Índice de Gravedad  = dias_perdidos / accidentes  (si hay accidentes)
    tasa_frecuencia = round((total_accidentes / horas_trabajadas) * 1_000_000, 2) if horas_trabajadas else 0
    tasa_severidad  = round((total_dias / horas_trabajadas) * 1_000, 2)           if horas_trabajadas else 0
    tasa_incidencia = round((total_eventos / total_empleados) * 100, 2)           if total_empleados  else 0
    indice_gravedad = round(total_dias / total_accidentes, 2)                     if total_accidentes else 0

    # Costo promedio por evento
    costo_promedio = round(total_costo / total_eventos, 2) if total_eventos else 0

    # ── POR DEPARTAMENTO ────────────────────────────────────
    por_departamento = {}
    if 'departamento' in col:
        for depto, grp in data.groupby(col['departamento']):
            empl = int(grp['_empl'].iloc[0]) if 'empleados_dept' in col else 0
            por_departamento[str(depto)] = {
                'eventos':   int(len(grp)),
                'accidentes':int((grp['_tipo'] == 'Accidente').sum()),
                'dias':      int(grp['_dias'].sum()),
                'costo':     round(float(grp['_costo'].sum()), 2),
                'empleados': empl,
            }

    # ── POR TIPO DE EVENTO ──────────────────────────────────
    por_tipo_evento = {
        'Accidente':   total_accidentes,
        'Enfermedad':  total_enfermedades,
        'Prevención':  total_prevencion,
    }

    # ── POR GRAVEDAD ────────────────────────────────────────
    por_gravedad = {}
    if 'gravedad' in col:
        for grav, grp in data.groupby(col['gravedad']):
            if str(grav).upper() in ('N/A', 'NAN', ''):
                continue
            por_gravedad[str(grav)] = {
                'eventos': int(len(grp)),
                'dias':    int(grp['_dias'].sum()),
                'costo':   round(float(grp['_costo'].sum()), 2),
            }

    # ── TOP 5 DIAGNÓSTICOS ──────────────────────────────────
    top_diagnosticos = []
    if 'diagnostico' in col:
        diag_counts = data[col['diagnostico']].value_counts().head(5)
        for diag, cnt in diag_counts.items():
            subset = data[data[col['diagnostico']] == diag]
            top_diagnosticos.append({
                'diagnostico': str(diag),
                'casos':       int(cnt),
                'dias':        int(subset['_dias'].sum()),
                'costo':       round(float(subset['_costo'].sum()), 2),
            })

    # ── TENDENCIA MENSUAL ───────────────────────────────────
    tendencia_mensual = []
    if 'fecha' in col and data['_fecha'].notna().any():
        data['_mes']  = data['_fecha'].dt.month
        data['_anio'] = data['_fecha'].dt.year
        for (anio, mes), grp in data.groupby(['_anio', '_mes']):
            tendencia_mensual.append({
                'mes':         MESES_ES.get(int(mes), str(mes)),
                'anio':        int(anio),
                'mes_num':     int(mes),
                'eventos':     int(len(grp)),
                'accidentes':  int((grp['_tipo'] == 'Accidente').sum()),
                'enfermedades':int((grp['_tipo'] == 'Enfermedad').sum()),
                'dias':        int(grp['_dias'].sum()),
                'costo':       round(float(grp['_costo'].sum()), 2),
            })
        tendencia_mensual.sort(key=lambda x: (x['anio'], x['mes_num']))

    # ── ACCIDENTES REPORTADOS A LA ARL ─────────────────────
    reportados_arl = 0
    if 'reportado_arl' in col:
        reportados_arl = int(
            data[col['reportado_arl']].astype(str).str.lower().str.strip().isin(['sí','si','yes','1','true']).sum()
        )

    # ── CASOS RESUELTOS ─────────────────────────────────────
    casos_resueltos = 0
    total_casos_activos = 0
    if 'resuelto' in col:
        resuelto_series     = data[col['resuelto']].astype(str).str.lower().str.strip()
        casos_resueltos     = int(resuelto_series.isin(['sí','si','yes','1','true']).sum())
        total_casos_activos = int(resuelto_series.isin(['no','0','false']).sum())

    # ── RESULTADO FINAL ─────────────────────────────────────
    return {
        # Totales generales
        'total_eventos':        total_eventos,
        'total_accidentes':     total_accidentes,
        'total_enfermedades':   total_enfermedades,
        'total_prevencion':     total_prevencion,
        'total_dias_perdidos':  total_dias,
        'total_costo':          total_costo,
        'total_empleados':      total_empleados,
        'horas_trabajadas':     horas_trabajadas,

        # Indicadores de seguridad (Decreto 522-06 / Ley 87-01)
        'tasa_frecuencia':      tasa_frecuencia,
        'tasa_severidad':       tasa_severidad,
        'tasa_incidencia':      tasa_incidencia,
        'indice_gravedad':      indice_gravedad,
        'costo_promedio_evento':costo_promedio,

        # Cumplimiento
        'reportados_arl':       reportados_arl,
        'casos_resueltos':      casos_resueltos,
        'casos_activos':        total_casos_activos,

        # Desgloses para gráficos
        'por_tipo_evento':      por_tipo_evento,
        'por_departamento':     por_departamento,
        'por_gravedad':         por_gravedad,
        'top_diagnosticos':     top_diagnosticos,
        'tendencia_mensual':    tendencia_mensual,

        # Meta
        'nota_normativa': (
            'Indicadores calculados según Ley 87-01 y Decreto 522-06 de República Dominicana. '
            f'Base: {HORAS_POR_EMPLEADO_ANIO:,} horas/empleado/año (44 h/semana × 50 semanas).'
        )
    }