-- ============================================
-- KPIs - Incidentes Viales Medellin
-- Base de datos: dw_incidentes_v2
-- ODS 3 - Salud y Bienestar
-- ============================================

-- KPI 1: Total de incidentes por año (tendencia general)
SELECT 
    t.anio,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
GROUP BY t.anio
ORDER BY t.anio;

-- KPI 2: Distribucion por gravedad del accidente
SELECT 
    a.gravedad_accidente,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM fact_incidente f
JOIN dim_accidente a ON f.sk_accidente = a.sk_accidente
GROUP BY a.gravedad_accidente
ORDER BY total DESC;

-- KPI 3: Top 10 comunas con mas incidentes
SELECT 
    u.comuna,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_ubicacion u ON f.sk_ubicacion = u.sk_ubicacion
WHERE u.comuna != 'DESCONOCIDO'
GROUP BY u.comuna
ORDER BY total_incidentes DESC
LIMIT 10;

-- KPI 4: Incidentes en dias festivos vs dias normales
SELECT 
    CASE WHEN t.es_festivo = 1 THEN 'Dia Festivo' ELSE 'Dia Normal' END AS tipo_dia,
    COUNT(*) AS total_incidentes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
GROUP BY t.es_festivo;

-- KPI 5: Estacionalidad mensual de incidentes
SELECT 
    t.mes,
    CASE t.mes
        WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
        WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
        WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
    END AS nombre_mes,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
GROUP BY t.mes
ORDER BY t.mes;

-- KPI 6: Incidentes por dia de semana
SELECT 
    t.dia_semana,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
GROUP BY t.dia_semana
ORDER BY total_incidentes DESC;

-- KPI 7: Incidentes por clase de accidente
SELECT 
    a.clase_accidente,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS porcentaje
FROM fact_incidente f
JOIN dim_accidente a ON f.sk_accidente = a.sk_accidente
GROUP BY a.clase_accidente
ORDER BY total DESC;

-- KPI 8: Incidentes por tipo de via (diseño)
SELECT 
    a.diseno_via,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_accidente a ON f.sk_accidente = a.sk_accidente
WHERE a.diseno_via != 'DESCONOCIDO'
GROUP BY a.diseno_via
ORDER BY total_incidentes DESC;

-- KPI 9: Tendencia anual de incidentes con muertos
SELECT 
    t.anio,
    COUNT(*) AS total_con_muertos
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
JOIN dim_accidente a ON f.sk_accidente = a.sk_accidente
WHERE a.gravedad_accidente = 'Con muertos'
GROUP BY t.anio
ORDER BY t.anio;

-- KPI 10: Top 10 festivos con mas incidentes
SELECT 
    t.nombre_festivo,
    COUNT(*) AS total_incidentes
FROM fact_incidente f
JOIN dim_tiempo t ON f.sk_tiempo = t.sk_tiempo
WHERE t.es_festivo = 1
GROUP BY t.nombre_festivo
ORDER BY total_incidentes DESC
LIMIT 10;
