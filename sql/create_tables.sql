-- ============================================
-- CREATE TABLES — Data Warehouse
-- Base de datos: dw_incidentes_v2
-- ODS 3 - Salud y Bienestar
-- Incidentes Viales Medellin 2014-2020
-- ============================================

CREATE DATABASE IF NOT EXISTS dw_incidentes_v2;
USE dw_incidentes_v2;

-- -----------------------------------------------
-- dim_tiempo
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS dim_tiempo (
    sk_tiempo        INT          NOT NULL AUTO_INCREMENT,
    fecha_accidente  DATE         NOT NULL,
    anio             INT          NOT NULL,
    mes              INT          NOT NULL,
    dia_semana       VARCHAR(20)  NOT NULL,
    es_fin_semana    TINYINT(1)   NOT NULL DEFAULT 0,
    nombre_festivo   VARCHAR(100) NOT NULL DEFAULT 'No festivo',
    tipo_festivo     VARCHAR(50)  NOT NULL DEFAULT 'N/A',
    es_festivo       TINYINT(1)   NOT NULL DEFAULT 0,
    PRIMARY KEY (sk_tiempo),
    INDEX idx_fecha (fecha_accidente),
    INDEX idx_anio (anio),
    INDEX idx_festivo (es_festivo)
);

-- -----------------------------------------------
-- dim_ubicacion
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    sk_ubicacion  INT          NOT NULL AUTO_INCREMENT,
    barrio        VARCHAR(100) NOT NULL DEFAULT 'DESCONOCIDO',
    comuna        VARCHAR(100) NOT NULL DEFAULT 'DESCONOCIDO',
    num_comuna    VARCHAR(20)  NOT NULL DEFAULT 'DESCONOCIDO',
    direccion     VARCHAR(200),
    x             FLOAT,
    y             FLOAT,
    PRIMARY KEY (sk_ubicacion),
    INDEX idx_comuna (comuna)
);

-- -----------------------------------------------
-- dim_accidente
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS dim_accidente (
    sk_accidente        INT          NOT NULL AUTO_INCREMENT,
    clase_accidente     VARCHAR(100) NOT NULL DEFAULT 'Otro',
    gravedad_accidente  VARCHAR(100) NOT NULL,
    diseno_via          VARCHAR(100) NOT NULL DEFAULT 'DESCONOCIDO',
    PRIMARY KEY (sk_accidente),
    INDEX idx_gravedad (gravedad_accidente),
    INDEX idx_clase (clase_accidente)
);

-- -----------------------------------------------
-- fact_incidente
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS fact_incidente (
    sk_incidente  INT          NOT NULL AUTO_INCREMENT,
    expediente    VARCHAR(50)  NOT NULL DEFAULT 'SIN_EXPEDIENTE',
    nro_radicado  VARCHAR(50),
    sk_tiempo     INT,
    sk_ubicacion  INT,
    sk_accidente  INT,
    PRIMARY KEY (sk_incidente),
    INDEX idx_sk_tiempo    (sk_tiempo),
    INDEX idx_sk_ubicacion (sk_ubicacion),
    INDEX idx_sk_accidente (sk_accidente),
    FOREIGN KEY (sk_tiempo)     REFERENCES dim_tiempo(sk_tiempo),
    FOREIGN KEY (sk_ubicacion)  REFERENCES dim_ubicacion(sk_ubicacion),
    FOREIGN KEY (sk_accidente)  REFERENCES dim_accidente(sk_accidente)
);
