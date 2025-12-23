{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_secao AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_secao') }}
)

SELECT *
FROM mstore_t_secao
