DO $$
BEGIN

    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'low_birth_weight'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_low_birth_weight_pc AS
        SELECT
            l.year,
            l.id_mun,
            SUM(l.confirmed)                          AS total_cases,
            p.population,
            ROUND(
                (SUM(l.total_cases)::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM low_birth_weight l
        LEFT JOIN population p
            ON l.id_mun = p.id_mun
            AND l.year  = p.year
        GROUP BY
            l.year,
            l.id_mun,
            p.population;

        RAISE NOTICE 'Vista v_low_birth_weight_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla low_birth_weight o population no existe';
    END IF;
END $$;