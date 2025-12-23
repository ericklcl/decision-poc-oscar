{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_marcafornecedor AS (
    
    SELECT
      idfornecedor,
      idmarcafornecedor,
      idmarca

    FROM {{ ref('mstore_t_marcafornecedor') }}

)

SELECT * 
FROM mstore_t_marcafornecedor