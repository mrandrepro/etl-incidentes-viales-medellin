"""
TRANSFORM v2 — Integra CSV + API festivos
Enriquece dim_tiempo con: es_festivo, nombre_festivo, dia_semana, es_fin_de_semana
"""
import pandas as pd
import sys
sys.path.append('.')
from extract_api import extract_festivos

RAW_PATH = r'C:\Users\pipei\OneDrive\Escritorio\proyecto_etl_v2\proyecto_etl_v2\1_raw_data\incidentes_viales.csv'

def extract_csv():
    print("=" * 50)
    print("EXTRAYENDO CSV...")
    df = pd.read_csv(RAW_PATH, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    print(f"Filas: {len(df):,} | Columnas: {df.shape[1]}")
    print("=" * 50)
    return df

def limpiar_csv(df):
    df = df.copy()
    df = df.drop_duplicates()
    df['AÑO'] = df['AÑO'].astype(str).str.strip().str.replace(r'\r', '', regex=True)
    df['AÑO'] = pd.to_numeric(df['AÑO'], errors='coerce')
    df['FECHA_ACCIDENTE'] = pd.to_datetime(df['FECHA_ACCIDENTE'], errors='coerce', dayfirst=True)
    # Normalizar a solo fecha (sin hora)
    df['FECHA_SOLO'] = df['FECHA_ACCIDENTE'].dt.normalize()
    df['GRAVEDAD_ACCIDENTE'] = df['GRAVEDAD_ACCIDENTE'].str.replace('Solo da\xf1os', 'Solo danos', regex=False)
    df['DISEÑO'] = df['DISEÑO'].str.replace('Pont\xf3n', 'Ponton', regex=False)
    clase_map = {
        'Caida Ocupante'    : 'Caida de Ocupante',
        'Caída Ocupante'    : 'Caida de Ocupante',
        'Caida de Ocupante' : 'Caida de Ocupante',
    }
    df['CLASE_ACCIDENTE'] = df['CLASE_ACCIDENTE'].replace(clase_map)
    df['BARRIO']          = df['BARRIO'].fillna('DESCONOCIDO')
    df['COMUNA']          = df['COMUNA'].fillna('DESCONOCIDO')
    df['NUMCOMUNA']       = df['NUMCOMUNA'].fillna('DESCONOCIDO')
    df['DISEÑO']          = df['DISEÑO'].fillna('DESCONOCIDO')
    df['CLASE_ACCIDENTE'] = df['CLASE_ACCIDENTE'].fillna('Otro')
    df = df.drop(columns=['FECHA_ACCIDENTES', 'CBML', 'LOCATION',
                           'DIRECCION ENCASILLADA'], errors='ignore')
    df['EXPEDIENTE']   = df['EXPEDIENTE'].fillna('SIN_EXPEDIENTE')
    df['NRO_RADICADO'] = df['NRO_RADICADO'].fillna(0)
    return df

def limpiar_festivos(df_festivos):
    df = df_festivos.copy()
    df['nombre_festivo'] = df['nombre_festivo'].fillna('Desconocido')
    df['tipo_festivo']   = df['tipo_festivo'].fillna('Public')
    df['fecha']          = pd.to_datetime(df['fecha']).dt.normalize()
    df = df.drop_duplicates(subset=['fecha'])
    return df

def transform():
    print("=" * 50)
    print("TRANSFORMANDO e INTEGRANDO datos...")

    df_inc      = limpiar_csv(extract_csv())
    df_festivos = limpiar_festivos(extract_festivos())

    # -----------------------------------------------
    # dim_tiempo — basada en fecha normalizada (sin hora)
    # -----------------------------------------------
    dim_tiempo = (
        df_inc[['FECHA_SOLO', 'AÑO', 'MES']]
        .drop_duplicates()
        .dropna(subset=['FECHA_SOLO'])
        .reset_index(drop=True)
    )

    # Enriquecer con día de semana y fin de semana
    dim_tiempo['dia_semana']    = dim_tiempo['FECHA_SOLO'].dt.day_name()
    dim_tiempo['es_fin_semana'] = dim_tiempo['FECHA_SOLO'].dt.dayofweek.isin([5, 6]).astype(int)

    # JOIN con festivos por fecha normalizada
    dim_tiempo = dim_tiempo.merge(
        df_festivos[['fecha', 'nombre_festivo', 'tipo_festivo']],
        left_on='FECHA_SOLO', right_on='fecha', how='left'
    )
    dim_tiempo['es_festivo']     = dim_tiempo['nombre_festivo'].notna().astype(int)
    dim_tiempo['nombre_festivo'] = dim_tiempo['nombre_festivo'].fillna('No festivo')
    dim_tiempo['tipo_festivo']   = dim_tiempo['tipo_festivo'].fillna('N/A')
    dim_tiempo = dim_tiempo.drop(columns=['fecha'], errors='ignore')

    dim_tiempo.insert(0, 'sk_tiempo', range(1, len(dim_tiempo) + 1))
    dim_tiempo.columns = [
        'sk_tiempo', 'fecha_accidente', 'anio', 'mes',
        'dia_semana', 'es_fin_semana', 'nombre_festivo',
        'tipo_festivo', 'es_festivo'
    ]
    print(f"dim_tiempo:    {len(dim_tiempo):,} filas (con festivos)")

    # -----------------------------------------------
    # dim_ubicacion
    # -----------------------------------------------
    dim_ubicacion = (
        df_inc[['BARRIO', 'COMUNA', 'NUMCOMUNA', 'DIRECCION', 'X', 'Y']]
        .drop_duplicates().reset_index(drop=True)
    )
    dim_ubicacion.insert(0, 'sk_ubicacion', range(1, len(dim_ubicacion) + 1))
    dim_ubicacion.columns = ['sk_ubicacion', 'barrio', 'comuna',
                              'num_comuna', 'direccion', 'x', 'y']
    print(f"dim_ubicacion: {len(dim_ubicacion):,} filas")

    # -----------------------------------------------
    # dim_accidente
    # -----------------------------------------------
    dim_accidente = (
        df_inc[['CLASE_ACCIDENTE', 'GRAVEDAD_ACCIDENTE', 'DISEÑO']]
        .drop_duplicates().reset_index(drop=True)
    )
    dim_accidente.insert(0, 'sk_accidente', range(1, len(dim_accidente) + 1))
    dim_accidente.columns = ['sk_accidente', 'clase_accidente',
                              'gravedad_accidente', 'diseno_via']
    print(f"dim_accidente: {len(dim_accidente):,} filas")

    # -----------------------------------------------
    # fact_incidente — join por FECHA_SOLO + AÑO + MES
    # -----------------------------------------------
    df_fact = df_inc.merge(
        dim_tiempo[['sk_tiempo', 'fecha_accidente', 'anio', 'mes']],
        left_on=['FECHA_SOLO', 'AÑO', 'MES'],
        right_on=['fecha_accidente', 'anio', 'mes'],
        how='left'
    )
    df_fact = df_fact.merge(
        dim_ubicacion,
        left_on=['BARRIO', 'COMUNA', 'NUMCOMUNA', 'DIRECCION', 'X', 'Y'],
        right_on=['barrio', 'comuna', 'num_comuna', 'direccion', 'x', 'y'],
        how='left'
    )
    df_fact = df_fact.merge(
        dim_accidente,
        left_on=['CLASE_ACCIDENTE', 'GRAVEDAD_ACCIDENTE', 'DISEÑO'],
        right_on=['clase_accidente', 'gravedad_accidente', 'diseno_via'],
        how='left'
    )

    fact_incidente = df_fact[['EXPEDIENTE', 'NRO_RADICADO',
                               'sk_tiempo', 'sk_ubicacion', 'sk_accidente']].copy()
    fact_incidente.insert(0, 'sk_incidente', range(1, len(fact_incidente) + 1))
    fact_incidente.columns = ['sk_incidente', 'expediente', 'nro_radicado',
                               'sk_tiempo', 'sk_ubicacion', 'sk_accidente']

    nulos_sk = fact_incidente['sk_tiempo'].isna().sum()
    print(f"fact_incidente:{len(fact_incidente):,} filas | sk_tiempo nulos: {nulos_sk}")
    print("=" * 50)

    return fact_incidente, dim_tiempo, dim_ubicacion, dim_accidente

if __name__ == '__main__':
    fact, dt, du, da = transform()
    print("\nMuestra dim_tiempo (festivos):")
    print(dt[dt['es_festivo'] == 1][['fecha_accidente','nombre_festivo','dia_semana']].head(5))
    print("\nMuestra fact_incidente:")
    print(fact.head())
    print("\nNulos en sk_tiempo:", fact['sk_tiempo'].isna().sum())