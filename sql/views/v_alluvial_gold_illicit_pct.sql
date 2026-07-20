-- =============================================================
-- Vista: v_alluvial_gold_illicit_pct
-- Descripción: Proportion of illicit alluvial gold exploitation
--              over total exploitation evidence, by municipality and year.
-- Source: alluvial_gold_mining (pipeline api_oro_aluvion)
-- =============================================================

CREATE OR REPLACE VIEW v_alluvial_gold_illicit_pct AS
SELECT
    year,
    id_mun,
    ROUND(
        CAST(illicit_hectares / total_evidence * 100 AS NUMERIC),
        2
    ) AS illicit_pct
FROM alluvial_gold_mining
WHERE total_evidence > 0;
