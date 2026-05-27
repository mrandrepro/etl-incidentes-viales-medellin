"""
AIRFLOW DAG — ETL Incidentes Viales Medellin v2
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')

RAW_PATH = '/opt/airflow/data/incidentes_viales.csv'

default_args = {
    'owner': 'etl_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def task_extract_csv(**kwargs):
    import pandas as pd
    df = pd.read_csv(RAW_PATH, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    print(f"CSV extraido: {df.shape[0]:,} filas")

def task_extract_api(**kwargs):
    import requests
    import pandas as pd
    registros = []
    for anio in range(2014, 2021):
        url = f"https://date.nager.at/api/v3/PublicHolidays/{anio}/CO"
        response = requests.get(url)
        for item in response.json():
            registros.append({'fecha': item['date'], 'nombre_festivo': item['localName']})
    df = pd.DataFrame(registros)
    print(f"Festivos extraidos: {len(df)}")

def task_transform(**kwargs):
    import pandas as pd
    import requests

    # Extract CSV
    df = pd.read_csv(RAW_PATH, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    df = df.drop_duplicates()
    df['AÑO'] = df['AÑO'].astype(str).str.strip().str.replace(r'\r', '', regex=True)
    df['AÑO'] = pd.to_numeric(df['AÑO'], errors='coerce')
    df['FECHA_ACCIDENTE'] = pd.to_datetime(df['FECHA_ACCIDENTE'], errors='coerce', dayfirst=True)
    df['FECHA_SOLO'] = df['FECHA_ACCIDENTE'].dt.normalize()
    df['BARRIO']          = df['BARRIO'].fillna('DESCONOCIDO')
    df['COMUNA']          = df['COMUNA'].fillna('DESCONOCIDO')
    df['NUMCOMUNA']       = df['NUMCOMUNA'].fillna('DESCONOCIDO')
    df['DISEÑO']          = df['DISEÑO'].fillna('DESCONOCIDO')
    df['CLASE_ACCIDENTE'] = df['CLASE_ACCIDENTE'].fillna('Otro')
    df['EXPEDIENTE']      = df['EXPEDIENTE'].fillna('SIN_EXPEDIENTE')

    # Extract festivos
    registros = []
    for anio in range(2014, 2021):
        url = f"https://date.nager.at/api/v3/PublicHolidays/{anio}/CO"
        for item in requests.get(url).json():
            registros.append({'fecha': item['date'], 'nombre_festivo': item['localName']})
    df_festivos = pd.DataFrame(registros)
    df_festivos['fecha'] = pd.to_datetime(df_festivos['fecha']).dt.normalize()

    # dim_tiempo
    dim_tiempo = df[['FECHA_SOLO','AÑO','MES']].drop_duplicates().dropna(subset=['FECHA_SOLO']).reset_index(drop=True)
    dim_tiempo['dia_semana']    = dim_tiempo['FECHA_SOLO'].dt.day_name()
    dim_tiempo['es_fin_semana'] = dim_tiempo['FECHA_SOLO'].dt.dayofweek.isin([5,6]).astype(int)
    dim_tiempo = dim_tiempo.merge(df_festivos, left_on='FECHA_SOLO', right_on='fecha', how='left')
    dim_tiempo['es_festivo']     = dim_tiempo['nombre_festivo'].notna().astype(int)
    dim_tiempo['nombre_festivo'] = dim_tiempo['nombre_festivo'].fillna('No festivo')
    dim_tiempo.insert(0, 'sk_tiempo', range(1, len(dim_tiempo)+1))

    print(f"Transform completo. dim_tiempo: {len(dim_tiempo):,} filas")

def task_validate(**kwargs):
    import pandas as pd
    import requests

    df = pd.read_csv(RAW_PATH, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    df['EXPEDIENTE'] = df['EXPEDIENTE'].fillna('SIN_EXPEDIENTE')

    errores = []
    if df['EXPEDIENTE'].isna().sum() > 0:
        errores.append("CRITICO: expediente tiene nulos")
    if df.duplicated().sum() > 100:
        errores.append("CRITICO: demasiados duplicados")

    if errores:
        raise Exception(f"Validacion fallida: {errores}")
    print("VALIDACION EXITOSA")

def task_load(**kwargs):
    from sqlalchemy import create_engine
    import pandas as pd
    import requests

    DB_URL = 'mysql+pymysql://root:@host.docker.internal:3306/dw_incidentes_v2'
    engine = create_engine(DB_URL)

    df = pd.read_csv(RAW_PATH, encoding='utf-8', on_bad_lines='skip', low_memory=False)
    df['EXPEDIENTE'] = df['EXPEDIENTE'].fillna('SIN_EXPEDIENTE')

    registros = []
    for anio in range(2014, 2021):
        url = f"https://date.nager.at/api/v3/PublicHolidays/{anio}/CO"
        for item in requests.get(url).json():
            registros.append({'fecha': item['date'], 'nombre_festivo': item['localName']})
    df_festivos = pd.DataFrame(registros)
    df_festivos['fecha'] = pd.to_datetime(df_festivos['fecha']).dt.normalize()

    df['FECHA_ACCIDENTE'] = pd.to_datetime(df['FECHA_ACCIDENTE'], errors='coerce', dayfirst=True)
    df['FECHA_SOLO'] = df['FECHA_ACCIDENTE'].dt.normalize()
    df['AÑO'] = pd.to_numeric(df['AÑO'].astype(str).str.strip().str.replace(r'\r','',regex=True), errors='coerce')

    dim_tiempo = df[['FECHA_SOLO','AÑO','MES']].drop_duplicates().dropna(subset=['FECHA_SOLO']).reset_index(drop=True)
    dim_tiempo['dia_semana']    = dim_tiempo['FECHA_SOLO'].dt.day_name()
    dim_tiempo['es_fin_semana'] = dim_tiempo['FECHA_SOLO'].dt.dayofweek.isin([5,6]).astype(int)
    dim_tiempo = dim_tiempo.merge(df_festivos, left_on='FECHA_SOLO', right_on='fecha', how='left')
    dim_tiempo['es_festivo']     = dim_tiempo['nombre_festivo'].notna().astype(int)
    dim_tiempo['nombre_festivo'] = dim_tiempo['nombre_festivo'].fillna('No festivo')
    dim_tiempo.insert(0, 'sk_tiempo', range(1, len(dim_tiempo)+1))
    dim_tiempo.columns = ['sk_tiempo','fecha_accidente','anio','mes','dia_semana','es_fin_semana','nombre_festivo','fecha','es_festivo']
    dim_tiempo = dim_tiempo.drop(columns=['fecha'], errors='ignore')

    dim_tiempo.to_sql('dim_tiempo', engine, if_exists='replace', index=False)
    print(f"Carga completa. dim_tiempo: {len(dim_tiempo):,} filas")

with DAG(
    dag_id='etl_incidentes_viales_v2',
    default_args=default_args,
    description='ETL incidentes viales Medellin + festivos Colombia',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'medellin', 'ods3'],
) as dag:

    t1 = PythonOperator(task_id='extract_csv',               python_callable=task_extract_csv)
    t2 = PythonOperator(task_id='extract_api_festivos',      python_callable=task_extract_api)
    t3 = PythonOperator(task_id='transform_integrate',       python_callable=task_transform)
    t4 = PythonOperator(task_id='validate_great_expectations', python_callable=task_validate)
    t5 = PythonOperator(task_id='load_to_dw',                python_callable=task_load)

    [t1, t2] >> t3 >> t4 >> t5
