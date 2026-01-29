

WITH grupo_norm AS (
  SELECT DISTINCT
    REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') AS nome_raw,
    CASE 
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'OI%' THEN 'OI'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'CLARO%' THEN 'CLARO'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'VIVO%' OR REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'TELEFÔNICA%' THEN 'VIVO'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'TIM%' THEN 'TIM'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'CTBC%' OR REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'ALGAR%' THEN 'ALGAR'
      WHEN REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g') LIKE 'SERCOMTEL%' THEN 'SERCOMTEL'
      ELSE REGEXP_REPLACE(TRIM(REGEXP_REPLACE(UPPER(grupo_economico), '\s*\([^)]*\)|\*+', '', 'g')), '\s+', ' ', 'g')
    END AS nome_final
  FROM "ida_datamart"."ida"."staging_ida"
)
SELECT DISTINCT nome_final AS nome_grupo
FROM grupo_norm