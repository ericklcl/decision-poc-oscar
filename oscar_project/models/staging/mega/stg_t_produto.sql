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

), mstore_t_produto AS (

    SELECT
      ecommerce,
      foradelinha,
      idcor,
      iddepartamento,
      idproduto,
      idreferenciaproduto,
      idtamanho,
      TRIM(INITCAP(nome))         AS nome,
      precovenda,
      precobase,
      primeiroprecovenda,
      status,
      valorcusto,
      closeout,
      ean,
      mto.idsituacaoproduto,
      s.descricao                 AS flg_status_produto,
      nomeimpressao
    FROM {{ ref('mstore_t_produto') }}  mto
    LEFT JOIN situacaoproduto s ON s.idsituacaoproduto = mto.idsituacaoproduto
)

SELECT *
FROM mstore_t_produto