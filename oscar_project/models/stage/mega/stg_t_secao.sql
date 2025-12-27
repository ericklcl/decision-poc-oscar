{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_secao AS (
    
    SELECT
      TRIM(status)                              AS status,
      idsecao,
      TRIM(INITCAP(descricao))                  AS descricao

    FROM {{ ref('mstore_t_secao') }}
    
)

SELECT * 
FROM mstore_t_secao