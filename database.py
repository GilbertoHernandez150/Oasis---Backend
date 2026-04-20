import sqlite3
import json
from datetime import datetime

DB_PATH = "database.db"


# ============================================================
# CONEXIÓN
# ============================================================
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Permite acceder columnas por nombre
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ============================================================
# INICIALIZAR BASE DE DATOS (crear tablas si no existen)
# ============================================================
def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # ----------------------------------------------------------
    # TABLA: usuarios
    # Preparada para login futuro. Por ahora se usa usuario_id=1
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre      TEXT    NOT NULL,
            email       TEXT    UNIQUE,
            empresa     TEXT,
            activo      INTEGER NOT NULL DEFAULT 1,
            creado_en   TEXT    NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # Insertar usuario por defecto si no existe
    cursor.execute("""
        INSERT OR IGNORE INTO usuarios (id, nombre, email, empresa)
        VALUES (1, 'Usuario Principal', 'admin@oasis.com', 'Mi Empresa')
    """)

    # ----------------------------------------------------------
    # TABLA: archivos
    # Registro de cada archivo cargado al sistema
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archivos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id      INTEGER NOT NULL DEFAULT 1,
            nombre_archivo  TEXT    NOT NULL,
            tipo_modulo     TEXT    NOT NULL,   -- 'economico' | 'sanitario'
            filas           INTEGER,
            columnas        TEXT,               -- JSON: lista de nombres de columnas
            moneda_detectada TEXT,
            cargado_en      TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
    """)

    # ----------------------------------------------------------
    # TABLA: analisis_economico
    # Guarda los resultados del análisis financiero
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analisis_economico (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            archivo_id          INTEGER NOT NULL,
            usuario_id          INTEGER NOT NULL DEFAULT 1,
            total_ingresos      REAL,
            total_gastos        REAL,
            balance             REAL,
            total_transacciones INTEGER,
            promedio_ingreso    REAL,
            promedio_gasto      REAL,
            isr                 REAL,
            afp                 REAL,
            sfs                 REAL,
            srl                 REAL,
            total_impuestos     REAL,
            ingreso_neto        REAL,
            top_ingresos        TEXT,   -- JSON
            top_gastos          TEXT,   -- JSON
            metodos_pago        TEXT,   -- JSON
            analizado_en        TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (archivo_id)  REFERENCES archivos(id),
            FOREIGN KEY (usuario_id)  REFERENCES usuarios(id)
        )
    """)

    # ----------------------------------------------------------
    # TABLA: analisis_sanitario
    # Guarda los resultados del módulo de Gestión Sanitaria
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analisis_sanitario (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            archivo_id              INTEGER NOT NULL,
            usuario_id              INTEGER NOT NULL DEFAULT 1,
            total_eventos           INTEGER,
            total_dias_perdidos     INTEGER,
            total_costo             REAL,
            accidentes              INTEGER,
            enfermedades            INTEGER,
            prevencion              INTEGER,
            tasa_frecuencia         REAL,   -- (accidentes / horas trabajadas) * 1,000,000
            tasa_severidad          REAL,   -- (dias_perdidos / horas trabajadas) * 1,000
            tasa_incidencia         REAL,   -- (eventos / empleados) * 100
            costo_promedio_evento   REAL,
            por_departamento        TEXT,   -- JSON: {dept: {eventos, dias, costo}}
            por_tipo_evento         TEXT,   -- JSON: {tipo: conteo}
            top_diagnosticos        TEXT,   -- JSON: lista top 5
            tendencia_mensual       TEXT,   -- JSON: [{mes, eventos, dias, costo}]
            analizado_en            TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (archivo_id)  REFERENCES archivos(id),
            FOREIGN KEY (usuario_id)  REFERENCES usuarios(id)
        )
    """)

    # ----------------------------------------------------------
    # TABLA: configuracion_dashboard
    # Guarda qué opciones seleccionó el usuario para cada análisis
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS configuracion_dashboard (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id      INTEGER NOT NULL DEFAULT 1,
            analisis_id     INTEGER NOT NULL,
            tipo_modulo     TEXT    NOT NULL,   -- 'economico' | 'sanitario'
            opciones        TEXT    NOT NULL,   -- JSON: lista de opciones seleccionadas
            sub_opciones    TEXT,               -- JSON: sub-opciones (ej: impuestos)
            creado_en       TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
    """)

    # ----------------------------------------------------------
    # TABLA: historial
    # Log de todas las operaciones del sistema
    # ----------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historial (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id  INTEGER NOT NULL DEFAULT 1,
            accion      TEXT    NOT NULL,   -- 'carga_archivo' | 'analisis' | 'exportar' | 'limpiar'
            modulo      TEXT,               -- 'economico' | 'sanitario'
            detalle     TEXT,               -- descripción legible
            metadata    TEXT,               -- JSON: datos adicionales
            creado_en   TEXT    NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Base de datos inicializada correctamente")


# ============================================================
# FUNCIONES: ARCHIVOS
# ============================================================

def guardar_archivo(nombre_archivo, tipo_modulo, filas, columnas, moneda_detectada, usuario_id=1):
    """
    Registra un archivo cargado. Retorna el ID del archivo guardado.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO archivos (usuario_id, nombre_archivo, tipo_modulo, filas, columnas, moneda_detectada)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        usuario_id,
        nombre_archivo,
        tipo_modulo,
        filas,
        json.dumps(columnas),
        moneda_detectada
    ))

    archivo_id = cursor.lastrowid
    conn.commit()
    conn.close()

    registrar_historial(
        accion="carga_archivo",
        modulo=tipo_modulo,
        detalle=f"Archivo '{nombre_archivo}' cargado ({filas} filas)",
        metadata={"archivo_id": archivo_id, "columnas": columnas},
        usuario_id=usuario_id
    )

    return archivo_id


def obtener_archivos(usuario_id=1, tipo_modulo=None, limit=20):
    """
    Retorna el historial de archivos cargados por el usuario.
    """
    conn = get_connection()
    cursor = conn.cursor()

    if tipo_modulo:
        cursor.execute("""
            SELECT * FROM archivos
            WHERE usuario_id = ? AND tipo_modulo = ?
            ORDER BY cargado_en DESC
            LIMIT ?
        """, (usuario_id, tipo_modulo, limit))
    else:
        cursor.execute("""
            SELECT * FROM archivos
            WHERE usuario_id = ?
            ORDER BY cargado_en DESC
            LIMIT ?
        """, (usuario_id, limit))

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Deserializar columnas
    for row in rows:
        if row.get("columnas"):
            row["columnas"] = json.loads(row["columnas"])

    return rows


# ============================================================
# FUNCIONES: ANÁLISIS ECONÓMICO
# ============================================================

def guardar_analisis_economico(archivo_id, resultado, usuario_id=1):
    """
    Guarda los resultados del análisis económico.
    Retorna el ID del análisis guardado.
    """
    conn = get_connection()
    cursor = conn.cursor()

    imp = resultado.get("impuestos", {})
    tss = imp.get("tss", {})

    cursor.execute("""
        INSERT INTO analisis_economico (
            archivo_id, usuario_id,
            total_ingresos, total_gastos, balance,
            total_transacciones, promedio_ingreso, promedio_gasto,
            isr, afp, sfs, srl, total_impuestos, ingreso_neto,
            top_ingresos, top_gastos, metodos_pago
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        archivo_id,
        usuario_id,
        resultado.get("total_ingresos"),
        resultado.get("total_gastos"),
        resultado.get("balance"),
        resultado.get("total_transacciones"),
        resultado.get("promedio_ingreso"),
        resultado.get("promedio_gasto"),
        imp.get("isr"),
        tss.get("afp"),
        tss.get("sfs"),
        tss.get("srl"),
        imp.get("total_impuestos"),
        imp.get("ingreso_neto"),
        json.dumps(resultado.get("top_ingresos", [])),
        json.dumps(resultado.get("top_gastos", [])),
        json.dumps(resultado.get("metodos_pago", {}))
    ))

    analisis_id = cursor.lastrowid
    conn.commit()
    conn.close()

    registrar_historial(
        accion="analisis",
        modulo="economico",
        detalle=f"Análisis económico completado. Balance: RD$ {resultado.get('balance', 0):,.2f}",
        metadata={"analisis_id": analisis_id, "archivo_id": archivo_id},
        usuario_id=usuario_id
    )

    return analisis_id


def obtener_analisis_economico(analisis_id=None, archivo_id=None, usuario_id=1, limit=10):
    """
    Retorna análisis económicos. Si se pasa analisis_id retorna uno específico.
    """
    conn = get_connection()
    cursor = conn.cursor()

    if analisis_id:
        cursor.execute("""
            SELECT ae.*, a.nombre_archivo, a.cargado_en as fecha_archivo
            FROM analisis_economico ae
            JOIN archivos a ON ae.archivo_id = a.id
            WHERE ae.id = ?
        """, (analisis_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        result = dict(row)
        # Deserializar JSON
        for campo in ["top_ingresos", "top_gastos", "metodos_pago"]:
            if result.get(campo):
                result[campo] = json.loads(result[campo])
        return result

    else:
        query = """
            SELECT ae.*, a.nombre_archivo
            FROM analisis_economico ae
            JOIN archivos a ON ae.archivo_id = a.id
            WHERE ae.usuario_id = ?
        """
        params = [usuario_id]
        if archivo_id:
            query += " AND ae.archivo_id = ?"
            params.append(archivo_id)
        query += " ORDER BY ae.analizado_en DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        for row in rows:
            for campo in ["top_ingresos", "top_gastos", "metodos_pago"]:
                if row.get(campo):
                    row[campo] = json.loads(row[campo])
        return rows


# ============================================================
# FUNCIONES: ANÁLISIS SANITARIO
# ============================================================

def guardar_analisis_sanitario(archivo_id, resultado, usuario_id=1):
    """
    Guarda los resultados del análisis sanitario.
    Retorna el ID del análisis guardado.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO analisis_sanitario (
            archivo_id, usuario_id,
            total_eventos, total_dias_perdidos, total_costo,
            accidentes, enfermedades, prevencion,
            tasa_frecuencia, tasa_severidad, tasa_incidencia,
            costo_promedio_evento,
            por_departamento, por_tipo_evento, top_diagnosticos, tendencia_mensual
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        archivo_id,
        usuario_id,
        resultado.get("total_eventos"),
        resultado.get("total_dias_perdidos"),
        resultado.get("total_costo"),
        resultado.get("accidentes"),
        resultado.get("enfermedades"),
        resultado.get("prevencion"),
        resultado.get("tasa_frecuencia"),
        resultado.get("tasa_severidad"),
        resultado.get("tasa_incidencia"),
        resultado.get("costo_promedio_evento"),
        json.dumps(resultado.get("por_departamento", {})),
        json.dumps(resultado.get("por_tipo_evento", {})),
        json.dumps(resultado.get("top_diagnosticos", [])),
        json.dumps(resultado.get("tendencia_mensual", []))
    ))

    analisis_id = cursor.lastrowid
    conn.commit()
    conn.close()

    registrar_historial(
        accion="analisis",
        modulo="sanitario",
        detalle=f"Análisis sanitario completado. Eventos: {resultado.get('total_eventos', 0)}",
        metadata={"analisis_id": analisis_id, "archivo_id": archivo_id},
        usuario_id=usuario_id
    )

    return analisis_id


def obtener_analisis_sanitario(analisis_id=None, archivo_id=None, usuario_id=1, limit=10):
    """
    Retorna análisis sanitarios. Si se pasa analisis_id retorna uno específico.
    """
    conn = get_connection()
    cursor = conn.cursor()

    if analisis_id:
        cursor.execute("""
            SELECT ans.*, a.nombre_archivo, a.cargado_en as fecha_archivo
            FROM analisis_sanitario ans
            JOIN archivos a ON ans.archivo_id = a.id
            WHERE ans.id = ?
        """, (analisis_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        result = dict(row)
        for campo in ["por_departamento", "por_tipo_evento", "top_diagnosticos", "tendencia_mensual"]:
            if result.get(campo):
                result[campo] = json.loads(result[campo])
        return result

    else:
        query = """
            SELECT ans.*, a.nombre_archivo
            FROM analisis_sanitario ans
            JOIN archivos a ON ans.archivo_id = a.id
            WHERE ans.usuario_id = ?
        """
        params = [usuario_id]
        if archivo_id:
            query += " AND ans.archivo_id = ?"
            params.append(archivo_id)
        query += " ORDER BY ans.analizado_en DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        for row in rows:
            for campo in ["por_departamento", "por_tipo_evento", "top_diagnosticos", "tendencia_mensual"]:
                if row.get(campo):
                    row[campo] = json.loads(row[campo])
        return rows


# ============================================================
# FUNCIONES: CONFIGURACIÓN DASHBOARD
# ============================================================

def guardar_configuracion_dashboard(analisis_id, tipo_modulo, opciones, sub_opciones=None, usuario_id=1):
    """
    Guarda la configuración seleccionada por el usuario para un dashboard.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO configuracion_dashboard (usuario_id, analisis_id, tipo_modulo, opciones, sub_opciones)
        VALUES (?, ?, ?, ?, ?)
    """, (
        usuario_id,
        analisis_id,
        tipo_modulo,
        json.dumps(opciones),
        json.dumps(sub_opciones) if sub_opciones else None
    ))

    config_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return config_id


def obtener_configuracion_dashboard(analisis_id, tipo_modulo):
    """
    Retorna la última configuración guardada para un análisis específico.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM configuracion_dashboard
        WHERE analisis_id = ? AND tipo_modulo = ?
        ORDER BY creado_en DESC
        LIMIT 1
    """, (analisis_id, tipo_modulo))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    result = dict(row)
    result["opciones"] = json.loads(result["opciones"])
    if result.get("sub_opciones"):
        result["sub_opciones"] = json.loads(result["sub_opciones"])
    return result


# ============================================================
# FUNCIONES: HISTORIAL
# ============================================================

def registrar_historial(accion, modulo=None, detalle=None, metadata=None, usuario_id=1):
    """
    Registra una acción en el historial del sistema.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO historial (usuario_id, accion, modulo, detalle, metadata)
        VALUES (?, ?, ?, ?, ?)
    """, (
        usuario_id,
        accion,
        modulo,
        detalle,
        json.dumps(metadata) if metadata else None
    ))

    conn.commit()
    conn.close()


def obtener_historial(usuario_id=1, limit=50, modulo=None):
    """
    Retorna el historial de acciones del usuario.
    """
    conn = get_connection()
    cursor = conn.cursor()

    if modulo:
        cursor.execute("""
            SELECT * FROM historial
            WHERE usuario_id = ? AND modulo = ?
            ORDER BY creado_en DESC
            LIMIT ?
        """, (usuario_id, modulo, limit))
    else:
        cursor.execute("""
            SELECT * FROM historial
            WHERE usuario_id = ?
            ORDER BY creado_en DESC
            LIMIT ?
        """, (usuario_id, limit))

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    for row in rows:
        if row.get("metadata"):
            row["metadata"] = json.loads(row["metadata"])

    return rows


# ============================================================
# FUNCIONES: ESTADÍSTICAS GENERALES
# ============================================================

def obtener_estadisticas_generales(usuario_id=1):
    """
    Retorna un resumen estadístico para el dashboard de inicio.
    """
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # Total de archivos cargados
    cursor.execute("SELECT COUNT(*) as total FROM archivos WHERE usuario_id = ?", (usuario_id,))
    stats["total_archivos"] = cursor.fetchone()["total"]

    # Total de análisis económicos
    cursor.execute("SELECT COUNT(*) as total FROM analisis_economico WHERE usuario_id = ?", (usuario_id,))
    stats["total_analisis_economicos"] = cursor.fetchone()["total"]

    # Total de análisis sanitarios
    cursor.execute("SELECT COUNT(*) as total FROM analisis_sanitario WHERE usuario_id = ?", (usuario_id,))
    stats["total_analisis_sanitarios"] = cursor.fetchone()["total"]

    # Último análisis económico
    cursor.execute("""
        SELECT ae.analizado_en, ae.balance, a.nombre_archivo
        FROM analisis_economico ae
        JOIN archivos a ON ae.archivo_id = a.id
        WHERE ae.usuario_id = ?
        ORDER BY ae.analizado_en DESC LIMIT 1
    """, (usuario_id,))
    row = cursor.fetchone()
    stats["ultimo_economico"] = dict(row) if row else None

    # Último análisis sanitario
    cursor.execute("""
        SELECT ans.analizado_en, ans.total_eventos, a.nombre_archivo
        FROM analisis_sanitario ans
        JOIN archivos a ON ans.archivo_id = a.id
        WHERE ans.usuario_id = ?
        ORDER BY ans.analizado_en DESC LIMIT 1
    """, (usuario_id,))
    row = cursor.fetchone()
    stats["ultimo_sanitario"] = dict(row) if row else None

    conn.close()
    return stats


# ============================================================
# FUNCIONES: LIMPIEZA
# ============================================================

def limpiar_sesion_actual(usuario_id=1):
    """
    Elimina el archivo y análisis más reciente del usuario.
    Útil para el botón 'Limpiar Datos' del frontend.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Obtener el último archivo
    cursor.execute("""
        SELECT id FROM archivos WHERE usuario_id = ?
        ORDER BY cargado_en DESC LIMIT 1
    """, (usuario_id,))
    row = cursor.fetchone()

    if row:
        archivo_id = row["id"]
        cursor.execute("DELETE FROM analisis_economico WHERE archivo_id = ?", (archivo_id,))
        cursor.execute("DELETE FROM analisis_sanitario WHERE archivo_id = ?", (archivo_id,))
        cursor.execute("DELETE FROM archivos WHERE id = ?", (archivo_id,))
        conn.commit()

    conn.close()
    registrar_historial(
        accion="limpiar",
        detalle="Sesión actual limpiada",
        usuario_id=usuario_id
    )


# ============================================================
# INICIALIZACIÓN AUTOMÁTICA
# ============================================================
if __name__ == "__main__":
    init_db()
    print("📊 Tablas creadas:")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for row in cursor.fetchall():
        print(f"   ✅ {row['name']}")
    conn.close()