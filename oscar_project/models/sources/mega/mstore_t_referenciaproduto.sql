{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_referenciaproduto AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_referenciaproduto') }}
)

SELECT *
FROM mstore_t_referenciaproduto
