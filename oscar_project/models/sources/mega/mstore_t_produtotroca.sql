{{ 
  config(
    materialized = 'table'
  ) 
}}

WITH produtotroca AS (

    SELECT
      idprodutotroca,
      status,
      idproduto,
      quantidade,
      idtroca,
      valor,
      idmotivotroca,
      idusuariodevolucao,
      datadevolucao,
      observacao,
      foradelinha,
      closeout,
      codigoab,
      codigoexterno,
      customedioproduto,
      custobruto,
      idsituacaoproduto,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_produtotroca') }}

), produtotroca_history AS (

    SELECT
      idprodutotroca,
      status,
      idproduto,
      quantidade,
      idtroca,
      valor,
      idmotivotroca,
      idusuariodevolucao,
      datadevolucao,
      observacao,
      foradelinha,
      closeout,
      codigoab,
      codigoexterno,
      customedioproduto,
      NULL                      AS custobruto,
      idsituacaoproduto,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_history_t_produtotroca') }}
    WHERE datadevolucao >= '2021-01-01'

), union_produtotroca AS (

    SELECT *
    FROM produtotroca
    UNION ALL 
    SELECT *
    FROM produtotroca_history

)

SELECT *
FROM union_produtotroca