{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_metaprodutosportal AS (
    
    SELECT
      *

    FROM {{ ref('mstore_t_metaprodutosportal') }}

)

SELECT * 
FROM mstore_t_metaprodutosportal