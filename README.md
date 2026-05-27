# ETL Project — Incidentes Viales Medellín
**ODS 3 — Salud y Bienestar | Entrega Final**  
**Curso:** ETL (G01) | **Programa:** Ingeniería de Datos e IA  
**Universidad:** Autónoma de Occidente

---

## 1. Objetivo de negocio refinado y preguntas analíticas

**Problema de negocio:**  
Analizar los incidentes viales ocurridos en Medellín entre 2014 y 2020 para identificar patrones de accidentalidad por ubicación, tipo de accidente, gravedad, tiempo y condiciones especiales (días festivos), que permitan orientar decisiones de seguridad vial y prevención.

**¿Por qué esta API?**  
La API de festivos de Colombia (Nager.Date) enriquece el análisis temporal permitiendo determinar si los días festivos tienen mayor o menor accidentalidad que los días normales — una hipótesis relevante para políticas de seguridad vial.

**Preguntas analíticas:**
1. ¿En qué comunas y barrios ocurren más incidentes viales?
2. ¿Cuál es la clase de accidente más frecuente?
3. ¿Qué meses y años concentran mayor accidentalidad?
4. ¿Hay más accidentes en días festivos que en días normales?
5. ¿Qué festivo concentra más accidentes históricamente?
6. ¿Cuál es la tendencia anual de incidentes con muertos?
7. ¿En qué tipo de vía ocurren más accidentes graves?

**KPIs finales:**

| KPI | Descripción | Decisión que soporta |
|-----|-------------|----------------------|
| Total incidentes por año | Tendencia general | Evaluar evolución de la accidentalidad |
| % incidentes con muertos | Tasa de fatalidad | Priorizar zonas de intervención |
| Top 10 comunas | Ranking geográfico | Asignar recursos de seguridad |
| Festivos vs días normales | Comparación temporal | Reforzar presencia en fechas clave |
| Distribución por gravedad | Severidad del accidente | Dimensionar servicios de emergencia |
| Incidentes por clase | Tipo de accidente | Diseñar campañas de prevención |
| Top festivos con más accidentes | Días de mayor riesgo | Planear operativos especiales |

---

## 2. Arquitectura del sistema

```
Dataset CSV (270,765 filas)          API Nager.Date (festivos CO)
        ↓                                      ↓
    Extract CSV                          Extract API
        ↓                                      ↓
        └──────────── Transform ───────────────┘
                           ↓
                  Great Expectations
                    (Validación)
                           ↓
              MySQL Data Warehouse (dw_incidentes_v2)
                     ↓              ↓
              Power BI          Kafka Producer
            (Dashboard)              ↓
                                Kafka Topic
                                     ↓
                              Kafka Consumer
                           (Monitoreo en tiempo real)
```

**Orquestación:** Apache Airflow 2.8.1 en Docker  
**Stack:** Python · Pandas · MySQL · Airflow · Kafka · Great Expectations · Power BI

---

## 3. Fuentes de datos

### Dataset principal
- **Nombre:** Incidentes Viales Medellín
- **Fuente:** Datos Abiertos Colombia
- **Filas:** 270,765 | **Columnas:** 18
- **Período:** 2014 – 2020
- **Formato:** CSV

### API — Festivos de Colombia
- **Nombre:** Nager.Date Public Holidays API
- **URL:** `https://date.nager.at/api/v3/PublicHolidays/{year}/CO`
- **Sin API key, gratuita**
- **Datos:** fecha, nombre del festivo, tipo
- **Registros extraídos:** 124 festivos (2014–2020)
- **Justificación:** permite analizar si los días festivos tienen mayor accidentalidad, enriqueciendo dim_tiempo con `es_festivo`, `nombre_festivo`, `dia_semana` y `es_fin_semana`

---

## 4. Resumen EDA y profiling

### Dataset CSV
| Problema | Columna | Decisión |
|----------|---------|----------|
| 7% nulos | BARRIO | Imputar 'DESCONOCIDO' |
| 4.7% nulos | COMUNA | Imputar 'DESCONOCIDO' |
| Tipo object con \r | AÑO | Strip + castear a int |
| Encoding incorrecto | GRAVEDAD_ACCIDENTE | Unificar 'Solo daños' |
| 3 variantes | CLASE_ACCIDENTE | Normalizar a 'Caída de Ocupante' |
| 7 filas duplicadas | - | drop_duplicates() |
| Columna redundante | FECHA_ACCIDENTES | Eliminar |

### API Festivos
- Sin nulos
- Sin duplicados por fecha
- 18 festivos por año × 7 años = 126 registros (2 eliminados por duplicado de fecha)
- Integración por fecha normalizada (sin hora)

---

## 5. Estrategia de integración

- **Clave de matching:** `fecha` (DATE normalizada sin hora)
- **Tipo de join:** LEFT JOIN sobre `dim_tiempo`
- **Resultado:** columnas nuevas en `dim_tiempo`: `es_festivo` (0/1), `nombre_festivo`, `dia_semana`, `es_fin_semana`
- **Fechas sin festivo:** `nombre_festivo = 'No festivo'`, `es_festivo = 0`
- **Inconsistencias:** ninguna — la API devuelve fechas limpias en formato ISO

---

## 6. Modelo dimensional

### Grano
> **Una fila por incidente vial registrado en Medellín, identificado por su número de expediente, fecha y ubicación.**

### Tablas

**fact_incidente**
| Columna | Tipo | Descripción |
|---------|------|-------------|
| sk_incidente | INT (PK) | Surrogate key |
| expediente | VARCHAR | Clave natural |
| nro_radicado | VARCHAR | Número de radicado |
| sk_tiempo | INT (FK) | → dim_tiempo |
| sk_ubicacion | INT (FK) | → dim_ubicacion |
| sk_accidente | INT (FK) | → dim_accidente |

**dim_tiempo** (enriquecida con API)
| Columna | Tipo | Descripción |
|---------|------|-------------|
| sk_tiempo | INT (PK) | Surrogate key |
| fecha_accidente | DATE | Fecha del incidente |
| anio | INT | Año |
| mes | INT | Mes (1-12) |
| dia_semana | VARCHAR | Lunes-Domingo |
| es_fin_semana | INT | 0/1 |
| nombre_festivo | VARCHAR | Nombre del festivo o 'No festivo' |
| es_festivo | INT | 0/1 — dato de la API |

**dim_ubicacion**
| Columna | Tipo | Descripción |
|---------|------|-------------|
| sk_ubicacion | INT (PK) | Surrogate key |
| barrio | VARCHAR | Barrio |
| comuna | VARCHAR | Comuna |
| num_comuna | VARCHAR | Número de comuna |
| direccion | VARCHAR | Dirección |
| x / y | FLOAT | Coordenadas MAGNA-SIRGAS |

**dim_accidente**
| Columna | Tipo | Descripción |
|---------|------|-------------|
| sk_accidente | INT (PK) | Surrogate key |
| clase_accidente | VARCHAR | Tipo de accidente |
| gravedad_accidente | VARCHAR | Gravedad |
| diseno_via | VARCHAR | Tipo de vía |

### Cambios respecto a entrega 1
- `dim_tiempo` se enriqueció con 4 columnas de la API de festivos
- Se agregaron índices a `fact_incidente` para optimizar JOINs

---

## 7. Validación con Great Expectations

| Expectativa | Tipo | Justificación |
|-------------|------|---------------|
| sk_incidente único y no nulo | CRÍTICA | PK de la fact table |
| sk_tiempo, sk_ubicacion, sk_accidente no nulos | CRÍTICA | FKs obligatorias |
| expediente no nulo | CRÍTICA | Clave natural del negocio |
| mes entre 1 y 12 | CRÍTICA | Rango lógico |
| es_festivo en [0,1] | CRÍTICA | Campo binario |
| es_fin_semana en [0,1] | CRÍTICA | Campo binario |
| anio entre 2014-2022 | NO CRÍTICA | Rango esperado |
| nombre_festivo no nulo | NO CRÍTICA | Se imputa con 'No festivo' |

**Regla de control:** fallos críticos detienen el pipeline. No críticos se loguean.

---

## 8. Airflow DAG

```
[extract_csv]  ↘
                → [transform_integrate] → [validate_ge] → [load_to_dw]
[extract_api]  ↗
```

- **DAG ID:** `etl_incidentes_viales_v2`
- **Schedule:** `@monthly`
- **Executor:** LocalExecutor
- **Infraestructura:** Docker (apache/airflow:2.8.1)
- **Dependencias:** extract_csv y extract_api corren en paralelo → transform → validate → load

---

## 9. Kafka — Streaming de métricas

**Broker:** Apache Kafka con Zookeeper en Docker  
**Topic:** `metricas_incidentes`  
**Intervalo:** cada 10 segundos

**Métricas publicadas (desde el DW, no el CSV):**

| Métrica | Descripción |
|---------|-------------|
| `total_incidentes` | Conteo total de filas en fact_incidente |
| `festivos_vs_normales` | Incidentes en días festivos vs normales |
| `top_comunas` | Top 5 comunas con más incidentes |
| `distribucion_gravedad` | Distribución por gravedad del accidente |

---

## 10. Dashboard — Resultados e insights

Conectado a `dw_incidentes_v2` en MySQL (no al CSV).

**Visualizaciones:**
1. Total incidentes: 270,758
2. Incidentes con muertos: 1,482 (0.5%)
3. Top 10 comunas — La Candelaria lidera con 52,189
4. Distribución por gravedad — 55.4% con heridos
5. Incidentes por clase — Choque es el 60%
6. Incidentes por día de semana
7. Top festivos con más accidentes

**Insights clave:**
- La Candelaria concentra casi el doble de accidentes que la segunda comuna
- El 2017 fue el año con más accidentalidad
- Los choques representan 6 de cada 10 incidentes
- Los días festivos tienen menos accidentes (menor circulación)

---

## 11. Monitoreo en tiempo real (Kafka Consumer)

El consumer recibe métricas cada 10 segundos y las muestra en consola:
- Total acumulado de incidentes en el DW
- Proporción festivos vs normales en tiempo real
- Ranking actualizado de comunas
- Distribución de gravedad

---

## 12. Limitaciones y mejoras futuras

**Limitaciones:**
- El dataset cubre solo Medellín (no toda Colombia)
- Los datos llegan hasta 2020 — no hay datos más recientes disponibles
- La API de festivos no incluye eventos locales (ferias de Medellín, etc.)
- Kafka corre localmente — en producción debería estar en un clúster

**Mejoras futuras:**
- Integrar datos de toda Colombia
- Agregar API de clima (Open-Meteo) para correlacionar lluvia con accidentalidad
- Implementar alertas automáticas cuando el consumer detecte picos
- Migrar a PostgreSQL o BigQuery para mejor rendimiento
- Dashboard en tiempo real con Streamlit conectado al consumer de Kafka

---

## 13. Instrucciones de configuración

### Requisitos
- Python 3.12+
- Java 24 (para Kafka nativo)
- XAMPP con MySQL
- Docker Desktop
- Power BI Desktop

### Instalación
```bash
pip install -r requirements.txt
```

### Base de datos
1. Iniciar XAMPP → Start MySQL
2. La base `dw_incidentes_v2` se crea automáticamente al correr `load_v2.py`

---

## 14. Cómo correr el DAG de Airflow

```bash
# Levantar contenedores
docker compose up airflow-webserver airflow-scheduler -d

# Abrir interfaz
# http://localhost:8081
# Usuario: admin | Contraseña: admin

# Activar el DAG: etl_incidentes_viales_v2
# Click en el botón Play
```

---

## 15. Cómo correr Kafka

```bash
# Levantar Kafka y Zookeeper
docker compose up zookeeper kafka -d

# Terminal 1 — Producer
python kafka/kafka_producer.py

# Terminal 2 — Consumer
python kafka/kafka_consumer.py
```

---

## Business Objectives Achievement

| Objetivo de negocio | Pregunta analítica | KPI / Métrica | Evidencia | Decisión |
|--------------------|--------------------|---------------|-----------|----------|
| Identificar zonas de riesgo | ¿Dónde ocurren más accidentes? | Top 10 comunas | Dashboard — barras | Priorizar La Candelaria |
| Reducir fatalidades | ¿Cuántos accidentes son fatales? | % con muertos (0.5%) | Tarjeta dashboard | Reforzar atención de emergencias |
| Prevención en festivos | ¿Hay más accidentes en festivos? | Festivos vs normales | Kafka + Dashboard | Operativos en días normales |
| Tendencia temporal | ¿El problema mejora o empeora? | Incidentes por año | Gráfico de línea | 2020 baja por COVID |
| Tipo de accidente | ¿Qué tipo de accidente prevenir? | Clase de accidente | Barras dashboard | Campañas contra choques |
