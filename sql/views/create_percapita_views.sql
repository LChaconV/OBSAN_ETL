-- =============================================================
-- Descripción: Crea vistas de tasa per cápita (x 100.000 hab.)
-- =============================================================

DO $$
BEGIN

    -- -------------------------------------------------------
    -- Vista 1: Desnutrición aguda menores de 5 años
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'acute_malnutrition_5'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_acute_malnutrition_5_pc AS
        SELECT
            a.confirmed,
            a.condition_end,
            a.year,
            a.age,
            a.id_mun,
            a.total_cases,
            p.population,
            ROUND(
                (a.total_cases::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM acute_malnutrition_5 a
        LEFT JOIN population p
            ON a.id_mun = p.id_mun
            AND a.year  = p.year;

        RAISE NOTICE 'Vista v_acute_malnutrition_5_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla acute_malnutrition_5 o population no existe';
    END IF;


    -- -------------------------------------------------------
    -- Vista 2: Bajo peso al nacer
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'low_birth_weight'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_low_birth_weight_pc AS
        SELECT
            l.id_low_birth_weight,
            l.year,
            l.id_mun,
            l.total_cases,
            p.population,
            ROUND(
                (l.total_cases::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM low_birth_weight l
        LEFT JOIN population p
            ON l.id_mun = p.id_mun
            AND l.year  = p.year;

        RAISE NOTICE 'Vista v_low_birth_weight_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla low_birth_weight o population no existe';
    END IF;


    -- -------------------------------------------------------
    -- Vista 3: Mortalidad por desnutrición
    -- -------------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'mortality_malnutrition'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.tables WHERE table_name = 'population'
    ) THEN
        CREATE OR REPLACE VIEW v_mortality_malnutrition_pc AS
        SELECT
            m.year,
            m.age,
            m.id_mun,
            m.total_cases,
            p.population,
            ROUND(
                (m.total_cases::NUMERIC * 100000.0) / NULLIF(p.population, 0), 2
            ) AS total_cases_per_capita
        FROM mortality_malnutrition m
        LEFT JOIN population p
            ON m.id_mun = p.id_mun
            AND m.year  = p.year;

        RAISE NOTICE 'Vista v_mortality_malnutrition_pc creada OK';
    ELSE
        RAISE NOTICE 'OMITIDA: tabla mortality_malnutrition o population no existe';
    END IF;

    -- -------------------------------------------------------
    -- Vista 4: Educacion Escolar 
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