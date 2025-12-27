{{
  config(
    materialized = 'view'
  ) 
}}

WITH mstore_t_estabelecimento AS (

    SELECT
      cnpj,
      idendereco,
      idestabelecimento,
      idunidadenegocio,
      idgrupof,
      inscricaoestadual,
      TRIM(INITCAP(nomefantasia))                       AS nomefantasia,
      TRIM(INITCAP(razaosocial))                        AS razaosocial,
      sigla,
      status

    FROM {{ ref('mstore_t_estabelecimento') }}

)

SELECT *
FROM mstore_t_estabelecimento