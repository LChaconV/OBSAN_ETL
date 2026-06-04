-- =============================================================
-- Descripción: Inserta municipios faltantes en dim_divipola
--              que no están presentes en el shapefile fuente.
--              Se ejecuta después de cargar el archivo de municipios
--              en dw_divipola. No modifica registros existentes.
-- =============================================================

-- Maripana (Guainía) — código DIVIPOLA 94663
INSERT INTO dim_divipola (
    id_mun,
    name_mun,
    id_dept
)
SELECT
    '94663',
    'Maripana',
    '94'
WHERE NOT EXISTS (
    SELECT 1 FROM dim_divipola WHERE id_mun = '94663'
);
