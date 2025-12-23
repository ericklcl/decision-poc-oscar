{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_marcafornecedor AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_marcafornecedor') }}
)

SELECT *
FROM mstore_t_marcafornecedor
