{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produtogenerico AS (

    SELECT
      TRIM(INITCAP(descricao))    AS descricao,
      idprodutogenerico

    FROM {{ ref('mstore_t_produtogenerico') }}

)

SELECT *
FROM mstore_t_produtogenerico