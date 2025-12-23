{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_situacaoproduto AS (

    SELECT
      *

    FROM {{ source('raw_input', 'mstore_t_situacaoproduto') }}

)

SELECT *
FROM mstore_t_situacaoproduto