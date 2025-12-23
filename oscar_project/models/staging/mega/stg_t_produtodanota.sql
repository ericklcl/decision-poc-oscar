{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_produtodanota AS (
    
    SELECT
      quantidadeitensperdida,
      quantidade,
      idpedido,
      quantidadeitensextra,
      idproduto                             AS sk_idprodutonota,
      TRIM(UPPER(notafiscalcancelada))      AS notafiscalcancelada,
      TRIM(UPPER(foradelinha))              AS foradelinha,
      precodevenda,
      precodecusto                          AS vlr_precodecustonota,
      idprodutodanota,
      idnotafiscal,
      origem,
      desconto,
      precocustopedido,
      precodecustoreal,
      ipi,
      acrescimo

    FROM {{ ref('mstore_t_produtodanota') }}
)

SELECT *
FROM mstore_t_produtodanota