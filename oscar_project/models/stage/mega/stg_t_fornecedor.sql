{{
  config(
    materialized = 'table'
  ) 
}}

WITH mstore_t_fornecedor AS (

    SELECT
      cpfcnpj,
      idatividade,
      inscricaoestadual,
      idfornecedor,
      status,
      idtipopessoa,
      idendereco,
      TRIM(INITCAP(nomereduzido)) AS nomereduzido,
      TRIM(INITCAP(nome))         AS nome

    FROM {{ ref('mstore_t_fornecedor') }}

)

SELECT *
FROM mstore_t_fornecedor