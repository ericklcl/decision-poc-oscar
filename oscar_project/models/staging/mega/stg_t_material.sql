{{
  config(
    materialized = 'table'
  )
}}

WITH mstore_t_material AS (
    
    SELECT
      idmaterial,
      TRIM(INITCAP(descricao))              AS descricao

    FROM {{ ref('mstore_t_material') }}

)

SELECT * 
FROM mstore_t_material