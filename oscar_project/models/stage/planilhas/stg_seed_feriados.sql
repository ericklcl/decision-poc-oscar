{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH feriado AS (

    SELECT  
      TO_DATE(data, 'DD/MM/YYYY')       AS data,
      dia_semana                  AS dia_semana,
      feriado                           AS ds_feriado

    FROM {{ ref('seed_feriados') }}

)

SELECT *
FROM feriado