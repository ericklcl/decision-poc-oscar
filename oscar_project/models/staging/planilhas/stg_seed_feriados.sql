{{
  config(
    materialized = 'table',
    tags="daily"
  ) 
}}

WITH feriado AS (

    SELECT  
      TO_DATE(Data, 'DD/MM/YYYY')       AS data,
      "Dia da Semana"                   AS dia_semana,
      Feriado                           AS ds_feriado

    FROM {{ ref('seed_feriados') }}

)

SELECT *
FROM feriado