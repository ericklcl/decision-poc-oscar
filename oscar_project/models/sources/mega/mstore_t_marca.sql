{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_marca AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_marca') }}
)

SELECT *
FROM mstore_t_marca
