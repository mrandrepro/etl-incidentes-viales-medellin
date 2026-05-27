"""
LOAD v2 — Carga al DW MySQL XAMPP
"""
from sqlalchemy import create_engine, text

USUARIO    = 'root'
CONTRASENA = ''
HOST       = 'localhost'
PUERTO     = '3306'
BASE_DATOS = 'dw_incidentes_v2'

DB_URL = f'mysql+pymysql://{USUARIO}:{CONTRASENA}@{HOST}:{PUERTO}/{BASE_DATOS}'

def crear_base_de_datos():
    url_sin_db = f'mysql+pymysql://{USUARIO}:{CONTRASENA}@{HOST}:{PUERTO}'
    engine_temp = create_engine(url_sin_db)
    with engine_temp.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {BASE_DATOS}"))
        conn.commit()
    print(f"Base de datos '{BASE_DATOS}' lista.")

def load(fact, dim_tiempo, dim_ubicacion, dim_accidente):
    print("=" * 50)
    print("CARGANDO al Data Warehouse (MySQL)...")
    engine = create_engine(DB_URL)

    dim_tiempo.to_sql('dim_tiempo', engine, if_exists='replace', index=False)
    print(f"dim_tiempo cargada:     {len(dim_tiempo):,} filas")

    dim_ubicacion.to_sql('dim_ubicacion', engine, if_exists='replace', index=False)
    print(f"dim_ubicacion cargada:  {len(dim_ubicacion):,} filas")

    dim_accidente.to_sql('dim_accidente', engine, if_exists='replace', index=False)
    print(f"dim_accidente cargada:  {len(dim_accidente):,} filas")

    fact.to_sql('fact_incidente', engine, if_exists='replace', index=False, chunksize=5000)
    print(f"fact_incidente cargada: {len(fact):,} filas")

    print("=" * 50)
    print(f"CARGA COMPLETA en '{BASE_DATOS}'")
    print("=" * 50)

if __name__ == '__main__':
    from transform_v2 import transform
    from validate_ge import validar
    fact, dt, du, da = transform()
    validar(fact, dt, du, da)
    crear_base_de_datos()
    load(fact, dt, du, da)
