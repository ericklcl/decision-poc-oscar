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

), mstore_t_orcamentoproduto AS (

    SELECT
      idorcamento,
      idproduto,
      foradelinha,
      closeout,
      s.descricao                                                                                AS flg_status_produto,
      (CASE WHEN FORADELINHA = 'S' AND CLOSEOUT = 'N' THEN 'FL'    
            WHEN FORADELINHA = 'N' AND CLOSEOUT = 'S' THEN 'CO'    
            WHEN FORADELINHA = 'N' AND CLOSEOUT = 'N' THEN 'NORMAL' END)                         AS flg_status_produto_antiga,	
      decode(QUANTIDADE,null,0,QUANTIDADE)                                                       AS qtd_produto_vendido,
      decode(VALORVENDA,null,0,VALORVENDA)                                                       AS vlr_venda,
      decode(DESCONTOVALOR,null,0,DESCONTOVALOR)                                                 AS vlr_desconto,
      decode(VALORCUPOMDESCONTO,null,0,VALORCUPOMDESCONTO)                                       AS vlr_cupom,
      decode(VALORCASHBACK,null,0,VALORCASHBACK)                                                 AS vlr_cashback,
      decode(VALORDESCONTOFUNCIONARIO,null,0,VALORDESCONTOFUNCIONARIO)                           AS vlr_desconto_funcionario, 
      decode(VALORKITDESCONTO,null,0,VALORKITDESCONTO)                                           AS vlr_kit_desconto, 
      decode(CUSTOMEDIOPRODUTO,null,0,CUSTOMEDIOPRODUTO) * decode(QUANTIDADE,null,0,QUANTIDADE)  AS vlr_custo, 
      (decode(VALORVENDA,null,0,VALORVENDA)
      - decode(DESCONTOVALOR,null,0,DESCONTOVALOR)
      - decode(VALORKITDESCONTO,null,0,VALORKITDESCONTO)
      - decode(VALORCUPOMDESCONTO,null,0,VALORCUPOMDESCONTO))                                    AS vlr_venda_liquida,
      decode(CUSTOBRUTO,null,0,CUSTOBRUTO)                                                       AS vlr_custo_bruto 

    FROM {{ ref('mstore_t_orcamentoproduto') }} mto
    LEFT JOIN situacaoproduto s ON s.idsituacaoproduto = mto.idsituacaoproduto

)

SELECT *
FROM mstore_t_orcamentoproduto