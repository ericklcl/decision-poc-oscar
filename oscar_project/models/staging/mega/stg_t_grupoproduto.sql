{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_grupoproduto AS (
    
    SELECT
      status,
      idgrupoproduto,
      TRIM(INITCAP(descricao))                  AS descricao

    FROM {{ ref('mstore_t_grupoproduto') }}

)

SELECT * 
FROM mstore_t_grupoproduto