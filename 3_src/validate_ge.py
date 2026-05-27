"""
VALIDACION con Great Expectations
Valida fact_incidente y dimensiones antes de cargar al DW.
"""
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def validar_columna_no_nula(df, col, critico=True, errores=[]):
    nulos = df[col].isna().sum()
    if nulos > 0:
        msg = f"{'CRITICO' if critico else 'NO CRITICO'}: {col} tiene {nulos} nulos"
        if critico:
            errores.append(msg)
        else:
            log.warning(msg)

def validar_columna_unica(df, col, errores=[]):
    dups = df[col].duplicated().sum()
    if dups > 0:
        errores.append(f"CRITICO: {col} tiene {dups} duplicados")

def validar_rango(df, col, min_val, max_val, critico=True, errores=[]):
    fuera = df[(df[col] < min_val) | (df[col] > max_val)][col].count()
    if fuera > 0:
        msg = f"{'CRITICO' if critico else 'NO CRITICO'}: {col} tiene {fuera} valores fuera de rango [{min_val}-{max_val}]"
        if critico:
            errores.append(msg)
        else:
            log.warning(msg)

def validar_valores_permitidos(df, col, valores, errores=[]):
    invalidos = ~df[col].isin(valores)
    n = invalidos.sum()
    if n > 0:
        errores.append(f"CRITICO: {col} tiene {n} valores fuera de {valores}")

def validar(fact, dim_tiempo, dim_ubicacion, dim_accidente):
    print("=" * 50)
    print("VALIDANDO datos...")
    errores_criticos = []

    # --- fact_incidente ---
    validar_columna_no_nula(fact, 'sk_incidente', critico=True,  errores=errores_criticos)
    validar_columna_unica(fact, 'sk_incidente', errores=errores_criticos)
    validar_columna_no_nula(fact, 'sk_ubicacion', critico=True,  errores=errores_criticos)
    validar_columna_no_nula(fact, 'sk_accidente', critico=True,  errores=errores_criticos)
    validar_columna_no_nula(fact, 'expediente',   critico=True,  errores=errores_criticos)
    validar_columna_no_nula(fact, 'sk_tiempo',    critico=False, errores=errores_criticos)

    # --- dim_tiempo ---
    validar_columna_unica(dim_tiempo, 'sk_tiempo', errores=errores_criticos)
    validar_rango(dim_tiempo, 'mes',  1, 12,   critico=True,  errores=errores_criticos)
    validar_rango(dim_tiempo, 'anio', 2014, 2022, critico=False, errores=errores_criticos)
    validar_valores_permitidos(dim_tiempo, 'es_festivo',    [0, 1], errores=errores_criticos)
    validar_valores_permitidos(dim_tiempo, 'es_fin_semana', [0, 1], errores=errores_criticos)

    # --- dim_accidente ---
    validar_columna_no_nula(dim_accidente, 'clase_accidente',    critico=True, errores=errores_criticos)
    validar_columna_no_nula(dim_accidente, 'gravedad_accidente', critico=True, errores=errores_criticos)

    # --- Resultado ---
    print("=" * 50)
    if errores_criticos:
        print("VALIDACION FALLIDA — Pipeline detenido")
        for e in errores_criticos:
            print(f"  {e}")
        raise Exception(f"Validacion critica fallida: {len(errores_criticos)} error(es)")
    else:
        print("VALIDACION EXITOSA — Todos los checks criticos pasaron")
    print("=" * 50)
    return True

if __name__ == '__main__':
    from transform_v2 import transform
    fact, dt, du, da = transform()
    validar(fact, dt, du, da)
