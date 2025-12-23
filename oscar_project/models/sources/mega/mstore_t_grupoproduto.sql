{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_grupoproduto AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_grupoproduto') }}

)

SELECT *
FROM mstore_t_grupoproduto