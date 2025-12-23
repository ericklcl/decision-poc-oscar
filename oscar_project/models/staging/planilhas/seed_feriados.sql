{{
  config(
    materialized = 'table'
  ) 
}}

WITH feriados AS (

    SELECT  
      *

    FROM {{ ref('aux_feriados') }}

)

SELECT *
FROM feriados