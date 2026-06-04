-- =============================================================
-- Descripción: Crea vistas de tasa per cápita (x 100.000 hab.)
-- =============================================================

DO $$
BEGIN
    -- -------------------------------------------------------
    -- Desnutrición aguda menores de 5 años
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'acute_malnutrition_5'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_acute_malnutrition_5_pc AS
        SELECT
            a.year,
            a.id_mun,
            COUNT(*)::INTEGER        AS total_cases,
            p.population,
            ROUND(
                (COUNT(*)::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM acute_malnutrition_5 a
        LEFT JOIN population p
            ON a.id_mun = p.id_mun
            AND a.year  = p.year
        GROUP BY a.year, a.id_mun, p.population;

        RAISE NOTICE 'Vista v_acute_malnutrition_5_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla acute_malnutrition_5 o population no existe';
    END IF;

END $$;