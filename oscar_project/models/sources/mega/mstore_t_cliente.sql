{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_cliente AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_cliente') }}

)

SELECT *
FROM mstore_t_cliente