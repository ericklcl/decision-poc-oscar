{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produto AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_produto') }}

)

SELECT *
FROM mstore_t_produto