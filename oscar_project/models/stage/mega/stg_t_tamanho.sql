{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_tamanho AS (

    SELECT
      idtamanho,
      TRIM(INITCAP(descricao))    AS descricao

    FROM {{ ref('mstore_t_tamanho') }}
)

SELECT *
FROM mstore_t_tamanho