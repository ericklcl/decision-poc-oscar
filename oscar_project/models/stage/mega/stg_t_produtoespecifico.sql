{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produtoespecifico AS (

    SELECT
      TRIM(INITCAP(descricao))    AS descricao,
      idprodutoespecifico

    FROM {{ ref('mstore_t_produtoespecifico') }}

)

SELECT *
FROM mstore_t_produtoespecifico