{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_referenciaproduto AS (

    SELECT
      idmarca,
      idprodutoespecifico,
      idprodutogenerico,
      idreferenciaproduto,
      idgrupoproduto,
      idsecao,
      idmaterial,
      datacadastro,
      referencia,
      idcomprador,
      custobruto,
      idunidade,
      ncm,
      cest

    FROM {{ ref('mstore_t_referenciaproduto') }}

)

SELECT *
FROM mstore_t_referenciaproduto