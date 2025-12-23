{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_fornecedor AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_fornecedor') }}
)

SELECT *
FROM mstore_t_fornecedor
