{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_history_estoque AS (

    SELECT
      *
    FROM {{ source('raw_data', 'mstore_history_estoque') }}
)

SELECT *
FROM mstore_history_estoque