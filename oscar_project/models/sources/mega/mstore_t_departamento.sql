{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_departamento AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_departamento') }}
)

SELECT *
FROM mstore_t_departamento
