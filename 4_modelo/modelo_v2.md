# Modelo Dimensional v2

## Cambios respecto a la primera entrega
- Se agrega `dim_clima` con datos de la API Open-Meteo
- `fact_incidente` agrega columna `sk_clima` (FK a dim_clima)
- El grano no cambia

## dim_clima (NUEVA)
| Columna | Tipo | Descripcion |
|---|---|---|
| sk_clima | INT (PK) | Surrogate key |
| fecha | DATE | Fecha del registro climatico |
| precipitacion_mm | FLOAT | Precipitacion total del dia en mm |
| temp_max_c | FLOAT | Temperatura maxima en Celsius |
| temp_min_c | FLOAT | Temperatura minima en Celsius |
| viento_max_kmh | FLOAT | Velocidad maxima del viento |
| codigo_clima | INT | Codigo WMO del clima |
| condicion_clima | VARCHAR | Descripcion del clima (Lluvia, Tormenta, etc.) |

## Justificacion
La condicion climatica es un factor externo relevante para la accidentalidad.
Al unir por fecha se pueden responder preguntas como:
- Hay mas accidentes en dias de lluvia?
- El tipo de accidente cambia segun el clima?
