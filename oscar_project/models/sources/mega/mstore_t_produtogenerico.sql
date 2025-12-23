{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produtogenerico AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_produtogenerico') }}
)

SELECT *
FROM mstore_t_produtogenerico
