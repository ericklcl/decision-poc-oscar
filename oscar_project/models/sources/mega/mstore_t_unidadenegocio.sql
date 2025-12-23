{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_unidadenegocio AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_unidadenegocio') }}
)

SELECT *
FROM mstore_t_unidadenegocio
