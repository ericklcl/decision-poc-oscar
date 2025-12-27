{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_estoque AS (

    SELECT
      status,
      idestabelecimento                 AS nk_loja,
      idproduto                         AS nk_produto,
      precovenda                        AS vlr_precovenda,
      quantidade                        AS qtd_estoque,
      valorcusto                        AS vlr_valorcusto,
      valorcustomedio                   AS vlr_customedio,
      icms

    FROM {{ ref('mstore_t_estoque') }}

)

SELECT *
FROM mstore_t_estoque