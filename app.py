from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import pandas as pd
import os
import json
import hashlib
from datetime import datetime

from utils.currency_detector import detect_currency
from services.economic_analysis import analyze_economic_data
from services.health_analysis import analyze_health_data
from services.export_service import export_economico_xlsx, export_sanitario_xlsx
from utils.excel_service import clean_dataframe, safe_json_value
from database import (
    init_db,
    guardar_archivo,
    guardar_analisis_economico,
    guardar_analisis_sanitario,
    obtener_analisis_economico,
    guardar_configuracion_dashboard,
    obtener_configuracion_dashboard,
    obtener_estadisticas_generales,
    obtener_historial,
    limpiar_sesion_actual,
    registrar_historial
)

NETLIFY_URL = "https://oasisv1.netlify.app/"  # ← pon tu URL real

app = Flask(__name__)
CORS(app, origins=[NETLIFY_URL, "http://localhost:5500", "http://127.0.0.1:5500"])

init_db()

# ── Credenciales (en producción usar BD con hash) ───────
USUARIOS_VALIDOS = {
    'admin': hashlib.sha256('admin12345'.encode()).hexdigest()
}
USUARIOS_INFO = {
    'admin': {'nombre': 'Administrador', 'rol': 'admin'}
}

current_df         = None
current_archivo_id = None
current_modulo     = None

COLS_ECONOMICO = ['categoria','monto','ingreso','egreso','precio','venta','total','importe']
COLS_SANITARIO = ['tipo_evento','dias_perdidos','accidente','enfermedad','gravedad','diagnostico','departamento']


def detectar_modulo(df):
    cols      = [c.lower().replace(' ', '_') for c in df.columns]
    score_eco = sum(1 for c in cols if any(k in c for k in COLS_ECONOMICO))
    score_san = sum(1 for c in cols if any(k in c for k in COLS_SANITARIO))
    return 'sanitario' if score_san > score_eco else 'economico'


# Columnas mínimas OBLIGATORIAS para cada módulo
# El archivo debe tener AL MENOS una de cada grupo para ser válido
COLS_REQUERIDAS_ECONOMICO = {
    # Debe tener una columna de categoría/tipo de transacción
    'categoria_o_tipo':  ['categoria', 'category', 'ingreso', 'egreso', 'venta', 'transaccion'],
    # Debe tener una columna de monto/valor financiero (excluye 'costo' para no confundir con sanitario)
    'monto_o_valor':     ['monto', 'amount', 'precio', 'price', 'importe', 'valor', 'total_venta'],
}

COLS_REQUERIDAS_SANITARIO = {
    # Debe tener una columna que identifique el tipo de evento de salud
    'tipo_evento':       ['tipo_evento', 'event_type', 'accidente', 'enfermedad', 'incidente'],
    # Debe tener una columna de impacto (días perdidos, gravedad o diagnóstico)
    'impacto':           ['dias_perdidos', 'days_lost', 'gravedad', 'severity', 'diagnostico', 'diagnosis'],
}

# Umbral mínimo de coincidencia
SCORE_MINIMO = 1


def validar_columnas(df, modulo: str) -> dict:
    """
    Valida que el DataFrame tenga las columnas mínimas para el módulo dado.
    Retorna {"valido": True} o {"valido": False, "mensaje": "..."}
    """
    cols = [c.lower().replace(' ', '_').replace(' ', '_') for c in df.columns]

    if modulo == 'economico':
        requeridas = COLS_REQUERIDAS_ECONOMICO
        nombre_modulo = 'Finanzas y Operaciones'
        ejemplos = 'Categoria, Monto, Fecha, Descripcion, Metodo_Pago'
    elif modulo == 'sanitario':
        requeridas = COLS_REQUERIDAS_SANITARIO
        nombre_modulo = 'Gestión Sanitaria'
        ejemplos = 'Tipo_Evento, Dias_Perdidos, Departamento, Diagnostico, Gravedad'
    else:
        return {"valido": False, "mensaje": f"Módulo desconocido: {modulo}"}

    grupos_faltantes = []
    for grupo, aliases in requeridas.items():
        encontrado = any(
            any(alias in col for alias in aliases)
            for col in cols
        )
        if not encontrado:
            grupos_faltantes.append(grupo)

    if grupos_faltantes:
        cols_detectadas = ', '.join(list(df.columns)[:6])
        return {
            "valido": False,
            "mensaje": (
                f"Este archivo no es compatible con el módulo de {nombre_modulo}. "
                f"Las columnas detectadas ({cols_detectadas}...) no corresponden "
                f"al formato esperado. "
                f"Un archivo de {nombre_modulo} debe contener columnas como: {ejemplos}."
            )
        }

    return {"valido": True}


def safe_jsonify(data: dict, status: int = 200):
    """
    Versión segura de jsonify que convierte NaN/Infinity a null
    antes de serializar, evitando el error 'NaN is not valid JSON'.
    """
    cleaned = safe_json_value(data)
    response = app.response_class(
        response=json.dumps(cleaned, ensure_ascii=False),
        status=status,
        mimetype='application/json'
    )
    return response


# ============================================================
# RUTA RAÍZ — solo confirma que el backend está vivo
# ============================================================
@app.route('/')
def index():
    return safe_jsonify({"status": "OASIS Backend activo", "version": "1.0"})

# ============================================================
# ENDPOINT: LOGIN
# ============================================================
@app.route("/login", methods=["POST"])
def login():
    data     = request.get_json() or {}
    username = data.get('username', '').strip().lower()
    password = data.get('password', '')
    pwd_hash = hashlib.sha256(password.encode()).hexdigest()

    if username in USUARIOS_VALIDOS and USUARIOS_VALIDOS[username] == pwd_hash:
        info = USUARIOS_INFO.get(username, {})
        return safe_jsonify({
            "success": True,
            "user": {
                "username": username,
                "nombre":   info.get('nombre', username),
                "rol":      info.get('rol', 'usuario')
            }
        })
    return safe_jsonify({"success": False, "error": "Credenciales incorrectas"}, 401)


# ============================================================
# ENDPOINT: VERIFICAR SESIÓN
# ============================================================
@app.route("/check-session", methods=["GET"])
def check_session():
    # La sesión se maneja en el frontend con sessionStorage
    # Este endpoint es para verificaciones futuras con tokens
    return safe_jsonify({"ok": True})


# ============================================================
# ENDPOINT: SUBIR ARCHIVO
# ============================================================
@app.route("/upload", methods=["POST"])
def upload_file():
    global current_df, current_archivo_id, current_modulo

    if 'file' not in request.files:
        return safe_jsonify({"error": "No se encontró el archivo"}, 400)

    file     = request.files['file']
    filename = file.filename.lower()
    modulo   = request.form.get('modulo', 'auto')

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df_raw = pd.read_excel(file)
        elif filename.endswith(".csv"):
            df_raw = pd.read_csv(file)
        else:
            return safe_jsonify({"error": "Formato no soportado. Use .xlsx, .xls o .csv"}, 400)

        # ── Detectar módulo ──────────────────────────────────
        if modulo == 'auto':
            modulo = detectar_modulo(df_raw)

        # ── Limpiar datos ────────────────────────────────────
        limpieza = clean_dataframe(df_raw.copy(), modulo)
        if limpieza['error']:
            return safe_jsonify({"error": limpieza['error']}, 400)

        df_clean = limpieza['df']
        reporte  = limpieza['reporte']

        current_df     = df_clean
        current_modulo = modulo

        currency = detect_currency(df_clean)
        columnas = list(df_clean.columns)

        current_archivo_id = guardar_archivo(
            nombre_archivo   = file.filename,
            tipo_modulo      = modulo,
            filas            = len(df_clean),
            columnas         = columnas,
            moneda_detectada = currency
        )

        return safe_jsonify({
            "message":    "Archivo cargado correctamente",
            "archivo_id": current_archivo_id,
            "columns":    columnas,
            "rows":       len(df_clean),
            "currency":   currency,
            "modulo":     modulo,
            "limpieza":   reporte,
            "preview":    safe_json_value(df_clean.head(5).to_dict('records'))
        })

    except Exception as e:
        return safe_jsonify({"error": str(e)}, 500)


# ============================================================
# ENDPOINT: ANALIZAR DATOS
# ============================================================
@app.route("/analyze", methods=["POST"])
def analyze_data():
    global current_df, current_archivo_id, current_modulo

    if current_df is None:
        return safe_jsonify({"error": "No hay ningún archivo cargado."}, 400)

    try:
        data          = request.get_json() or {}
        analysis_type = data.get('type', current_modulo or 'economico')

        # ── Validar compatibilidad archivo ↔ módulo ──────────
        modulo_validar = 'economico' if analysis_type in ('economic','economico') else 'sanitario'
        validacion = validar_columnas(current_df, modulo_validar)
        if not validacion['valido']:
            return safe_jsonify({"error": validacion['mensaje']}, 400)

        # ── MÓDULO ECONÓMICO ─────────────────────────────────
        if analysis_type in ('economic', 'economico'):
            result = analyze_economic_data(current_df.copy())
            if 'error' in result:
                return safe_jsonify(result, 400)

            analisis_id = guardar_analisis_economico(
                archivo_id = current_archivo_id,
                resultado  = result
            )
            return safe_jsonify({
                "success":       True,
                "analysis_type": "economic",
                "analisis_id":   analisis_id,
                "archivo_id":    current_archivo_id,
                "data":          result
            })

        # ── MÓDULO SANITARIO ─────────────────────────────────
        elif analysis_type == 'sanitario':
            result = analyze_health_data(current_df.copy())
            if 'error' in result:
                return safe_jsonify(result, 400)

            analisis_id = guardar_analisis_sanitario(
                archivo_id = current_archivo_id,
                resultado  = result
            )
            return safe_jsonify({
                "success":       True,
                "analysis_type": "sanitario",
                "analisis_id":   analisis_id,
                "archivo_id":    current_archivo_id,
                "data":          result
            })

        else:
            return safe_jsonify({"error": f"Tipo no soportado: {analysis_type}"}, 400)

    except Exception as e:
        return safe_jsonify({"error": f"Error durante el análisis: {str(e)}"}, 500)


# ============================================================
# ENDPOINT: GUARDAR CONFIGURACIÓN DEL DASHBOARD
# ============================================================
@app.route("/dashboard/config", methods=["POST"])
def save_dashboard_config():
    try:
        data         = request.get_json() or {}
        analisis_id  = data.get('analisis_id')
        tipo_modulo  = data.get('tipo_modulo', 'economico')
        opciones     = data.get('opciones', [])
        sub_opciones = data.get('sub_opciones', [])

        if not analisis_id:
            return safe_jsonify({"success": True, "config_id": None})

        config_id = guardar_configuracion_dashboard(
            analisis_id  = analisis_id,
            tipo_modulo  = tipo_modulo,
            opciones     = opciones,
            sub_opciones = sub_opciones
        )
        return safe_jsonify({"success": True, "config_id": config_id})
    except Exception as e:
        return safe_jsonify({"error": str(e)}, 500)


# ============================================================
# ENDPOINTS RESTANTES
# ============================================================
@app.route("/analisis/economico/<int:analisis_id>", methods=["GET"])
def get_analisis_economico(analisis_id):
    try:
        result = obtener_analisis_economico(analisis_id=analisis_id)
        if not result:
            return safe_jsonify({"error": "Análisis no encontrado"}, 404)
        return safe_jsonify({"success": True, "data": result})
    except Exception as e:
        return safe_jsonify({"error": str(e)}, 500)


@app.route("/historial", methods=["GET"])
def get_historial():
    try:
        modulo = request.args.get('modulo')
        limit  = int(request.args.get('limit', 50))
        rows   = obtener_historial(modulo=modulo, limit=limit)
        return safe_jsonify({"success": True, "historial": rows})
    except Exception as e:
        return safe_jsonify({"error": str(e)}, 500)


@app.route("/estadisticas", methods=["GET"])
def get_estadisticas():
    try:
        stats = obtener_estadisticas_generales()
        return safe_jsonify({"success": True, "stats": stats})
    except Exception as e:
        return safe_jsonify({"error": str(e)}, 500)


@app.route("/current-file", methods=["GET"])
def get_current_file():
    if current_df is None:
        return safe_jsonify({"loaded": False, "message": "No hay archivo cargado"})
    return safe_jsonify({
        "loaded":     True,
        "archivo_id": current_archivo_id,
        "modulo":     current_modulo,
        "columns":    list(current_df.columns),
        "rows":       len(current_df),
    })


# ============================================================
# ENDPOINT: EXPORTAR EXCEL
# ============================================================
@app.route("/export/excel", methods=["GET"])
def export_excel():
    global current_df, current_modulo

    from database import obtener_analisis_economico, obtener_analisis_sanitario
    import io

    try:
        if current_modulo in ('economico', 'economic', None):
            rows = obtener_analisis_economico(usuario_id=1, limit=1)
            if not rows:
                return safe_jsonify({"error": "No hay análisis económico guardado. Genera un análisis primero."}), 404
            data       = rows[0]
            xlsx_bytes = export_economico_xlsx(data, data.get('nombre_archivo', 'analisis'))
            filename   = f"OASIS_Finanzas_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        else:
            rows = obtener_analisis_sanitario(usuario_id=1, limit=1)
            if not rows:
                return safe_jsonify({"error": "No hay análisis sanitario guardado. Genera un análisis primero."}), 404
            data       = rows[0]
            xlsx_bytes = export_sanitario_xlsx(data, data.get('nombre_archivo', 'analisis'))
            filename   = f"OASIS_Sanitario_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

        buffer = io.BytesIO(xlsx_bytes)
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/clear", methods=["POST"])
def clear_data():
    global current_df, current_archivo_id, current_modulo
    try:
        limpiar_sesion_actual()
    except Exception:
        pass
    current_df         = None
    current_archivo_id = None
    current_modulo     = None
    return safe_jsonify({"success": True, "message": "Datos limpiados correctamente"})


if __name__ == "__main__":
    print(f"🌴 OASIS corriendo en http://127.0.0.1:5000")
    print(f"📁 Frontend: {os.path.abspath(FRONTEND_DIR)}")
    app.run(debug=True, port=5000)