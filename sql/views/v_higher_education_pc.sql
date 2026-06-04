
DO $$
BEGIN
    -- -------------------------------------------------------
    -- Vista : Educacion Superior
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'higher_education'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_higher_education_pc AS
        SELECT
            h.id_higher_education,
            h.year,
            h.id_mun,

            h.prof_technician,
            h.technologist,
            h.university,
            h.specialization,
            h.master,
            h.doctorate,

            COALESCE(h.prof_technician, 0)
            + COALESCE(h.technologist,  0)
            + COALESCE(h.university,    0)
            + COALESCE(h.specialization,0)
            + COALESCE(h.master,        0)
            + COALESCE(h.doctorate,     0) AS total_higher_education,

            p.population,

            ROUND(COALESCE(h.prof_technician, 0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_prof_technician,
            ROUND(COALESCE(h.technologist,    0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_technologist,
            ROUND(COALESCE(h.university,      0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_university,
            ROUND(COALESCE(h.specialization,  0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_specialization,
            ROUND(COALESCE(h.master,          0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_master,
            ROUND(COALESCE(h.doctorate,       0)  * 100.0 / NULLIF(p.population, 0), 2) AS pct_doctorate,

            ROUND(
                (
                    COALESCE(h.prof_technician, 0)
                    + COALESCE(h.technologist,  0)
                    + COALESCE(h.university,    0)
                    + COALESCE(h.specialization,0)
                    + COALESCE(h.master,        0)
                    + COALESCE(h.doctorate,     0)
                ) * 100.0 / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita

        FROM higher_education h
        LEFT JOIN population p
            ON h.id_mun = p.id_mun
            AND h.year  = p.year;

        RAISE NOTICE 'Vista v_higher_education_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla higher_education o population no existe';
    END IF;
END $$;