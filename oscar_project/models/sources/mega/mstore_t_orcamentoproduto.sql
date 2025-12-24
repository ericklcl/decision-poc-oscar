{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_orcamentoproduto AS (

    SELECT
      idorcamentoproduto,
      valorvenda,
      idorcamento,
      quantidade,
      idproduto,
      valorcusto,
      descontovalor,
      foradelinha,
      premio,
      closeout,
      codigoab,
      retiradodoestoquepelatroca,
      conferido,
      ehpresente,
      valorkitdesconto,
      ehkitproduto,
      tipokitprodutoaplicado,
      agrupamentopresente,
      ehbrinde,
      valorcashback,
      ehkitprodutobrinde,
      tipokitproduto,
      valorcupomdesconto,
      statusprodutonovo,
      valordescontofuncionario,
      freterateado,
      codigoexterno,
      customedioproduto,
      customedioprodutoconsolidado,
      CASE
        WHEN custobruto < valorcusto OR custobruto = 0 THEN valorcusto
        ELSE custobruto
      END AS custobruto,
      --custobruto,
      idsituacaoproduto,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_t_orcamentoproduto') }}

), orcamentoproduto_history AS (

    SELECT
      idorcamentoproduto,
      valorvenda,
      idorcamento,
      quantidade,
      idproduto,
      valorcusto,
      descontovalor,
      foradelinha,
      premio,
      closeout,
      codigoab,
      retiradodoestoquepelatroca,
      conferido,
      ehpresente,
      valorkitdesconto,
      ehkitproduto,
      tipokitprodutoaplicado,
      agrupamentopresente,
      ehbrinde,
      valorcashback,
      ehkitprodutobrinde,
      tipokitproduto,
      valorcupomdesconto,
      statusprodutonovo,
      valordescontofuncionario,
      freterateado,
      codigoexterno,
      customedioproduto,
      customedioprodutoconsolidado,
      NULL                            AS custobruto,
      idsituacaoproduto,
      _dms_loaded_at

    FROM {{ source('raw_input', 'mstore_history_t_orcamentoproduto') }}

), union_orcamentoproduto AS (

    SELECT *
    FROM mstore_t_orcamentoproduto
    UNION ALL 
    SELECT *
    FROM orcamentoproduto_history

)

SELECT *
FROM union_orcamentoproduto