{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produtoespecifico AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_produtoespecifico') }}
)

SELECT *
FROM mstore_t_produtoespecifico
