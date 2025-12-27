{{
  config(
    materialized = 'table'
  ) 
}}

WITH feriados AS (

    SELECT  
      to_date(data, 'DD/MM/YYYY') AS data,
      dia_da_semana,
      feriado

    FROM {{ ref('aux_feriados') }}

)

SELECT 
  data,
  dia_da_semana AS dia_semana,
  feriado AS ds_feriado
FROM feriados