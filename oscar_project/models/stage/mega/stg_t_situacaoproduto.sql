{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_situacaoproduto AS (

    SELECT
      *
    FROM {{ ref('mstore_t_situacaoproduto') }}

)

SELECT *
FROM mstore_t_situacaoproduto