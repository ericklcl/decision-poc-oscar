{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_grupof AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_grupof') }}
)

SELECT *
FROM mstore_t_grupof
