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
 
    FROM {{ source('raw_data', 'mstore_enriched_gender_classifier') }}
 
)
 
SELECT *
FROM mstore_gender_classifier