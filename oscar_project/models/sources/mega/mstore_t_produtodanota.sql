{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_produtodanota AS (

    SELECT
      idprodutodanota,
      ipi,
      precodevenda,
      precodecusto,
      quantidade,
      idproduto,
      idnotafiscal,
      precocustopedido,
      quantidadeitensextra,
      quantidadeitensperdida,
      idpedido,
      acrescimo,
      desconto,
      codigobarra,
      notafiscalcancelada,
      foradelinha,
      precodecustoreal,
      closeout,
      codigoab,
      cprod,
      ncm,
      origem,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_produtodanota') }}

), produtodanota_history AS (

    SELECT
      idprodutodanota,
      ipi,
      precodevenda,
      precodecusto,
      quantidade,
      idproduto,
      idnotafiscal,
      precocustopedido,
      quantidadeitensextra,
      quantidadeitensperdida,
      idpedido,
      acrescimo,
      desconto,
      codigobarra,
      notafiscalcancelada,
      foradelinha,
      precodecustoreal,
      closeout,
      codigoab,
      cprod,
      ncm,
      origem,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_history_t_produtodanota') }}

), union_produtodanota AS (

    SELECT *
    FROM mstore_t_produtodanota
    UNION ALL 
    SELECT *
    FROM produtodanota_history

)

SELECT *
FROM union_produtodanota