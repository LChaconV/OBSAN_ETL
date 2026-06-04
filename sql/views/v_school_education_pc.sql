
DO $$
BEGIN
    -- -------------------------------------------------------
    -- Vista: Educacion Escolar 
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'school_education'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_school_education_pc AS
        SELECT
            s.id_school_education,
            s.year,
            s.id_mun,
            s.total,
            p.population,
            ROUND(
                (s.total::NUMERIC * 100.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM school_education s
        LEFT JOIN population p
            ON s.id_mun = p.id_mun
            AND s.year  = p.year;

        RAISE NOTICE 'Vista v_school_education_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla school_education o population no existe';
    END IF;
END $$;