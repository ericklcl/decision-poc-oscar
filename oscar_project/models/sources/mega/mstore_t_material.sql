{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_material AS (

    SELECT
      *
    FROM {{ source('raw_input', 'mstore_t_material') }}
)

SELECT *
FROM mstore_t_material
