import pandas as pd
from utils.currency_detector import convert_to_rdp, clean_amount


# NORMALIZAR CATEGORIA
def normalize_category(value):
    value = str(value).lower().strip()
    if value in ['ingreso', 'ingresos', 'venta', 'entrada']:
        return 'ingreso'
    if value in ['gasto', 'gastos', 'egreso', 'salida']:
        return 'gasto'
    return 'otro'


# ANALISIS ECONOMICO
def analyze_economic_data(df):
    required_columns = ['Categoria', 'Monto']
    for col in required_columns:
        if col not in df.columns:
            return {"error": f"Columna requerida no encontrada: {col}"}
    
    # Normalizar categoria
    df['Categoria_Norm'] = df['Categoria'].apply(normalize_category)
    
    # Limpiar montos y manejar None/NaN
    df['Monto_Limpio'] = df['Monto'].apply(clean_amount).fillna(0)
    
    # Convertir a RD$
    if 'Moneda' in df.columns:
        df['Monto_RD'] = df.apply(
            lambda row: convert_to_rdp(row['Monto_Limpio'], row['Moneda']) or 0,
            axis=1
        )
    else:
        df['Monto_RD'] = df['Monto_Limpio']
    
    # Asegurar que no hay NaN
    df['Monto_RD'] = df['Monto_RD'].fillna(0)
    
    # Totales
    ingresos = df[df['Categoria_Norm'] == 'ingreso']['Monto_RD'].sum()
    gastos = df[df['Categoria_Norm'] == 'gasto']['Monto_RD'].sum()
    balance = ingresos - gastos
    impuestos = calculate_taxes_rd(ingresos)
    
    result = {
        'total_ingresos': round(float(ingresos), 2),
        'total_gastos': round(float(gastos), 2),
        'balance': round(float(balance), 2),
        'impuestos': impuestos,
        'total_transacciones': len(df),
        'promedio_ingreso': round(
            float(df[df['Categoria_Norm'] == 'ingreso']['Monto_RD'].mean() or 0), 2
        ),
        'promedio_gasto': round(
            float(df[df['Categoria_Norm'] == 'gasto']['Monto_RD'].mean() or 0), 2
        )
    }
    
    # Top movimientos
    if 'Descripcion' in df.columns:
        top_ing = df[df['Categoria_Norm'] == 'ingreso'].nlargest(5, 'Monto_RD')
        top_gas = df[df['Categoria_Norm'] == 'gasto'].nlargest(5, 'Monto_RD')
        
        result['top_ingresos'] = top_ing[['Descripcion', 'Monto_RD']].to_dict('records')
        result['top_gastos'] = top_gas[['Descripcion', 'Monto_RD']].to_dict('records')
    
    # Metodo de pago
    if 'Metodo_Pago' in df.columns:
        result['metodos_pago'] = df['Metodo_Pago'].value_counts().to_dict()
    
    return result


# IMPUESTOS RD
def calculate_taxes_rd(ingreso_anual):
    if ingreso_anual <= 416220:
        isr = 0
    elif ingreso_anual <= 624329:
        isr = (ingreso_anual - 416220) * 0.15
    elif ingreso_anual <= 867123:
        isr = (208109 * 0.15) + ((ingreso_anual - 624329) * 0.20)
    else:
        isr = (208109 * 0.15) + (242794 * 0.20) + ((ingreso_anual - 867123) * 0.25)
    
    tss = {
        'afp': ingreso_anual * 0.0287,
        'sfs': ingreso_anual * 0.0304,
        'srl': ingreso_anual * 0.001
    }
    total_tss = sum(tss.values())
    
    return {
        'isr': round(isr, 2),
        'tss': {k: round(v, 2) for k, v in tss.items()},
        'total_impuestos': round(isr + total_tss, 2),
        'ingreso_neto': round(ingreso_anual - isr - total_tss, 2),
        'nota_itbis': 'El ITBIS depende del tipo de actividad económica y no se calcula automáticamente'
    }