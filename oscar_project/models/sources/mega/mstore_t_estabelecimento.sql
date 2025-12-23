{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_estabelecimento AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_estabelecimento') }}
)

SELECT *
FROM mstore_t_estabelecimento
