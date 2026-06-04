DO $$
BEGIN

    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'mortality_malnutrition'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_mortality_malnutrition_pc AS
        SELECT
            m.year,
            m.id_mun,
            COUNT(*)::INTEGER                              AS total_cases,
            p.population,
            ROUND(
                (COUNT(*)::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM mortality_malnutrition m
        LEFT JOIN population p
            ON m.id_mun = p.id_mun
            AND m.year  = p.year
        GROUP BY
            m.year,
            m.id_mun,
            p.population;

        RAISE NOTICE 'Vista v_mortality_malnutrition_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla mortality_malnutrition o population no existe';
    END IF;
END $$;
