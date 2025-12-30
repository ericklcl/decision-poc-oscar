{{
  config(
    materialized = 'table'
  )
}}
 
WITH mstore_gender_classifier AS (
 
    SELECT
      idcliente,
      nome,
      classificacao
 
    FROM {{ ref('mstore_enriched_gender_classifier') }}
 
)
 
SELECT *
FROM mstore_gender_classifier