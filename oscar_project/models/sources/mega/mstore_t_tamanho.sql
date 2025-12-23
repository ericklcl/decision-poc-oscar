{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_tamanho AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_tamanho') }}
)

SELECT *
FROM mstore_t_tamanho
