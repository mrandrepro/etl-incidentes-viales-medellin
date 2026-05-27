"""
EXTRACT — API Nager.Date
Extrae festivos oficiales de Colombia por anio.
Sin API key, sin registro, 100% gratuita.
URL: https://date.nager.at/api/v3/PublicHolidays/{year}/CO
"""
import requests
import pandas as pd

def extract_festivos(anios=range(2014, 2021)):
    print("=" * 50)
    print("EXTRAYENDO festivos de Colombia (Nager.Date API)...")

    registros = []
    for anio in anios:
        url = f"https://date.nager.at/api/v3/PublicHolidays/{anio}/CO"
        response = requests.get(url)
        response.raise_for_status()
        for item in response.json():
            registros.append({
                'fecha'          : item['date'],
                'nombre_festivo' : item['localName'],
                'tipo_festivo'   : item['types'][0] if item.get('types') else 'Public'
            })
        print(f"  {anio}: {len(response.json())} festivos")

    df = pd.DataFrame(registros)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df = df.drop_duplicates(subset=['fecha'])

    print(f"Total festivos extraidos: {len(df)}")
    print("=" * 50)
    return df

if __name__ == '__main__':
    df = extract_festivos()
    print(df.head(10))
    df.to_csv(r'C:\Users\pipei\OneDrive\Escritorio\proyecto_etl_v2\proyecto_etl_v2\1_raw_data\festivos_colombia.csv', index=False)
    print("CSV guardado en 1_raw_data/festivos_colombia.csv")
