{{
  config(
    materialized = 'table'
  )
}}

WITH situacaoproduto AS (
  SELECT
    idsituacaoproduto,
    descricao
  FROM {{ ref('stg_t_situacaoproduto') }}

),mstore_t_produtotroca AS (
    
    SELECT
      idtroca,
      quantidade,
      customedioproduto,
      datadevolucao::DATE                               AS datadevolucao,
      status, --Verificar estes status
      idproduto,
      idmotivotroca,
      idprodutotroca,
      valor,
      foradelinha,
      custobruto,
      closeout,
      s.descricao                                                                           AS flg_status_produto,
      (CASE WHEN FORADELINHA = 'S' AND CLOSEOUT = 'N' THEN 'FL'
            WHEN FORADELINHA = 'N' AND CLOSEOUT = 'S' THEN 'CO'
            WHEN FORADELINHA = 'N' AND CLOSEOUT = 'N' THEN 'NORMAL' END)                    AS flg_status_produto_antiga


    FROM {{ ref('mstore_t_produtotroca') }} mto
    LEFT JOIN situacaoproduto s ON s.idsituacaoproduto = mto.idsituacaoproduto
)

SELECT * FROM mstore_t_produtotroca